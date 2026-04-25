"""
Daily health stream for airbyte-source-garmin.

Fetches one composite health summary per day from Garmin Connect for a given
date window and maps it to the flat schema defined in CLAUDE.md.

Raw Garmin API response quirks handled here:
  - Nested dict (lastNight) is flattened into top-level sleep fields
  - Null values for missing sensors (no HRV monitor, etc.) become None
  - Sanity check on resting heart rate (implausible values → None)
  - Integer coercion for step/sleep/calorie counts
"""

from datetime import date, timedelta
from typing import Any, Dict, Iterator, List, Optional

import garminconnect
import pandas as pd
from loguru import logger

from source_garmin.config import ConnectorConfig
from source_garmin.streams.base import GarminStream
from source_garmin.utils import retry_on_429

# ---------------------------------------------------------------------------
# Physiological sanity-check thresholds for resting heart rate.
# Resting HR of 20 bpm is seen in extreme endurance athletes (rare but real).
# Above 120 bpm at rest is clinically noteworthy — likely a sensor glitch.
# ---------------------------------------------------------------------------
_MIN_RHR_BPM: int = 20
_MAX_RHR_BPM: int = 120

# HRV (heart rate variability) reasonable range in milliseconds.
# Values outside this range almost certainly indicate a device artefact.
_MIN_HRV_MS: float = 5.0
_MAX_HRV_MS: float = 300.0


class DailyHealthStream(GarminStream):
    """Stream that yields one health summary record per calendar day.

    Supports both FULL_REFRESH and INCREMENTAL sync modes.
    The incremental cursor is `date` (ISO-8601 date string).

    Data is fetched by calling get_user_summary() once per day in the window.
    This one-call-per-day approach avoids the complexity of merging separate
    sleep, stress, and steps endpoints — get_user_summary() aggregates them.

    Field mapping:
        Garmin raw field              → Our schema field
        calendarDate                  → date
        totalSteps                    → steps
        restingHeartRate              → resting_heart_rate
        lastNight.sleepTimeSeconds    → sleep_seconds
        lastNight.deepSleepSeconds    → deep_sleep_seconds
        averageStressLevel            → stress_avg
        bodyBatteryChargedValue       → body_battery_charged
        bodyBatteryDrainedValue       → body_battery_drained
        activeKilocalories            → active_calories
        hrvWeeklyAverage              → hrv_avg
    """

    @property
    def name(self) -> str:
        """Stream name used in the Airbyte catalog and STATE messages."""
        return "daily_health"

    @property
    def primary_key(self) -> str:
        """Unique identifier: one record per calendar date."""
        return "date"

    @property
    def cursor_field(self) -> str:
        """Incremental cursor — the calendar date of the health summary."""
        return "date"

    # ------------------------------------------------------------------
    # JSON Schema — describes one record for the CATALOG message
    # ------------------------------------------------------------------

    def get_json_schema(self) -> Dict[str, Any]:
        """Return the JSON Schema (Draft-07) for one daily health record.

        All numeric fields are nullable because Garmin omits them when the
        corresponding sensor (HRV monitor, Body Battery, etc.) is absent or
        the device did not sync that day.

        Returns:
            A dict with {"type": "object", "properties": {...}}.
        """
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "date":                 {"type": ["string",  "null"], "format": "date"},
                "steps":                {"type": ["integer", "null"]},
                "resting_heart_rate":   {"type": ["integer", "null"]},
                "hrv_avg":              {"type": ["number",  "null"]},
                "sleep_seconds":        {"type": ["integer", "null"]},
                "deep_sleep_seconds":   {"type": ["integer", "null"]},
                "stress_avg":           {"type": ["integer", "null"]},
                "body_battery_charged": {"type": ["integer", "null"]},
                "body_battery_drained": {"type": ["integer", "null"]},
                "active_calories":      {"type": ["integer", "null"]},
                "ingested_at":          {"type": ["string",  "null"]},
            },
        }

    # ------------------------------------------------------------------
    # Core fetch — called by GarminStream.read()
    # ------------------------------------------------------------------

    def read_records(
        self,
        client: garminconnect.Garmin,
        config: ConnectorConfig,
        start_date: date,
        end_date: date,
    ) -> Iterator[Dict[str, Any]]:
        """Fetch one daily health summary per day and yield cleaned records.

        Iterates over each calendar date in [start_date, end_date] (inclusive),
        calls get_user_summary() once per date, normalises the nested response,
        then applies type coercion and sanity checks via pandas.

        A failed API call for one date is logged as a warning and skipped
        rather than aborting the whole stream — this handles days where Garmin
        returns a 404 (e.g. the user did not sync their watch that day).

        Args:
            client: An authenticated garminconnect.Garmin instance.
            config: Validated connector configuration (unused here but required
                by the base class contract).
            start_date: Inclusive start of the date window.
            end_date: Inclusive end of the date window (today).

        Yields:
            Flat dicts with the fields defined in get_json_schema(), excluding
            `ingested_at` (injected by the base class).
        """
        logger.info(
            "Fetching daily health summaries from Garmin API: {} → {}",
            start_date,
            end_date,
        )

        raw_records: List[Dict[str, Any]] = []
        current = start_date

        while current <= end_date:
            cdate = current.isoformat()
            try:
                daily = retry_on_429(lambda: client.get_user_summary(cdate))
            except Exception as exc:
                logger.warning(
                    "Could not fetch daily health summary for {} — skipping: {}",
                    cdate,
                    exc,
                )
                current += timedelta(days=1)
                continue

            if daily:
                raw_records.append(self._normalize_raw(daily))

            current += timedelta(days=1)

        if not raw_records:
            logger.info(
                "No daily health data returned for window {} → {}.",
                start_date,
                end_date,
            )
            return

        logger.info("Retrieved {} daily health record(s).", len(raw_records))

        df = pd.DataFrame(raw_records)
        df = self._transform(df)

        output_columns = [
            "date", "steps", "resting_heart_rate", "hrv_avg",
            "sleep_seconds", "deep_sleep_seconds", "stress_avg",
            "body_battery_charged", "body_battery_drained", "active_calories",
        ]
        df = df[output_columns]

        for _, row in df.iterrows():
            yield row.where(pd.notna(row), other=None).to_dict()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _normalize_raw(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten one raw Garmin daily summary into a simple key→value dict.

        The `lastNight` nested object is unpacked into top-level sleep fields
        so pandas can treat every column as a scalar series.

        Args:
            raw: A single daily summary dict from get_user_summary().

        Returns:
            A flat dict with string/number/None values only.
        """
        last_night = raw.get("lastNight") or {}

        return {
            "date":                 raw.get("calendarDate"),
            "steps":                raw.get("totalSteps"),
            "resting_heart_rate":   raw.get("restingHeartRate"),
            "sleep_seconds":        last_night.get("sleepTimeSeconds"),
            "deep_sleep_seconds":   last_night.get("deepSleepSeconds"),
            "stress_avg":           raw.get("averageStressLevel"),
            "body_battery_charged": raw.get("bodyBatteryChargedValue"),
            "body_battery_drained": raw.get("bodyBatteryDrainedValue"),
            "active_calories":      raw.get("activeKilocalories"),
            "hrv_avg":              raw.get("hrvWeeklyAverage"),
        }

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply type coercion and sanity checks to the normalised DataFrame.

        Args:
            df: Normalised DataFrame from _normalize_raw().

        Returns:
            A cleaned DataFrame with the same column names and correct types.
        """
        # ── Non-negative integer columns ──────────────────────────────────
        for col in ["steps", "sleep_seconds", "deep_sleep_seconds", "active_calories"]:
            df[col] = df[col].apply(
                lambda v: int(v) if pd.notna(v) and v is not None and v >= 0 else None
            )

        # ── Bounded integer columns (0–100 scale) ─────────────────────────
        # Garmin's stress and body battery are both 0–100 scores.
        for col in ["stress_avg", "body_battery_charged", "body_battery_drained"]:
            df[col] = df[col].apply(
                lambda v: int(v) if pd.notna(v) and v is not None and 0 <= v <= 100 else None
            )

        # ── Resting heart rate (sanity checked) ───────────────────────────
        df["resting_heart_rate"] = df["resting_heart_rate"].apply(self._check_rhr)

        # ── HRV (float, reasonable physiological range) ───────────────────
        df["hrv_avg"] = df["hrv_avg"].apply(
            lambda v: (
                round(float(v), 1)
                if pd.notna(v) and v is not None and _MIN_HRV_MS <= float(v) <= _MAX_HRV_MS
                else None
            )
        )

        # ── Nullable integer dtype to preserve int vs float in JSON output ─
        int_cols = [
            "steps", "resting_heart_rate", "sleep_seconds", "deep_sleep_seconds",
            "stress_avg", "body_battery_charged", "body_battery_drained", "active_calories",
        ]
        for col in int_cols:
            if col in df.columns:
                df[col] = df[col].astype(pd.Int64Dtype())

        return df

    def _check_rhr(self, value: Any) -> Optional[int]:
        """Validate a resting heart rate value is within physiological bounds.

        Values of 0 indicate no HR monitor data. Values outside the range
        [_MIN_RHR_BPM, _MAX_RHR_BPM] are almost certainly sensor artefacts.

        Args:
            value: Raw resting HR value from the API, or None/NaN.

        Returns:
            RHR as an integer if plausible, or None otherwise.
        """
        if pd.isna(value) or value is None or value == 0:
            return None
        try:
            rhr = int(value)
        except (TypeError, ValueError):
            return None

        if not (_MIN_RHR_BPM <= rhr <= _MAX_RHR_BPM):
            logger.warning(
                "Aberrant resting_heart_rate value {} bpm (expected {}-{}) — setting to None.",
                rhr,
                _MIN_RHR_BPM,
                _MAX_RHR_BPM,
            )
            return None

        return rhr
