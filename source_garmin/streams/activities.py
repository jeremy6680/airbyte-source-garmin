"""
Activities stream for airbyte-source-garmin.

Fetches Garmin Connect activity summaries (runs, rides, swims, etc.) for a
given date window and maps them to the flat schema defined in CLAUDE.md.

Raw Garmin API response quirks handled here:
  - Nested dicts (activityType, eventType) are flattened
  - Unit conversions: metres → km, seconds → minutes, m/s → min/km
  - Sanity checks: heart rate, pace, VO2max, training effect
  - Missing or aberrant values become None (never crash)
"""

from datetime import date, datetime
from typing import Any, Dict, Iterator, List, Optional

import garminconnect
import pandas as pd
from loguru import logger

from source_garmin.config import ConnectorConfig
from source_garmin.streams.base import GarminStream
from source_garmin.utils import retry_on_429

# ---------------------------------------------------------------------------
# Physiological sanity-check thresholds.
# Values outside these ranges are almost certainly sensor errors or API bugs.
# ---------------------------------------------------------------------------
_MIN_HR_BPM: int = 30         # Below this → no HR signal / GPS artefact
_MAX_HR_BPM: int = 250        # Above this → clearly an error
_MIN_SPEED_MS: float = 0.1    # ~0.36 km/h — anything slower is noise
_MAX_SPEED_MS: float = 13.0   # ~47 km/h — beyond any human running pace
_MAX_PACE_MIN_KM: float = 30.0  # 30 min/km → essentially stationary
_MIN_VO2MAX: float = 5.0
_MAX_VO2MAX: float = 100.0
_MIN_TRAINING_EFFECT: float = 0.0
_MAX_TRAINING_EFFECT: float = 5.0


class ActivitiesStream(GarminStream):
    """Stream that yields one record per Garmin Connect activity.

    Supports both FULL_REFRESH and INCREMENTAL sync modes.
    The incremental cursor is `activity_date` (ISO-8601 date string).

    Field mapping:
        Garmin raw field          → Our schema field
        activityId                → activity_id
        activityName              → activity_name
        startTimeLocal            → activity_date  (date portion only)
        activityType.typeKey      → activity_type
        eventType.typeKey         → event_type
        distance (m)              → distance_km
        duration (s)              → duration_minutes
        averageSpeed (m/s)        → avg_pace_min_km
        averageHR                 → avg_heart_rate
        maxHR                     → max_heart_rate
        elevationGain             → elevation_gain_m
        calories                  → calories
        averageRunningCadenceInStepsPerMinute → avg_cadence
        aerobicTrainingEffect     → training_effect
        vO2MaxValue               → vo2max_estimate
    """

    @property
    def name(self) -> str:
        """Stream name used in the Airbyte catalog and STATE messages."""
        return "activities"

    @property
    def primary_key(self) -> str:
        """Unique identifier for an activity record."""
        return "activity_id"

    @property
    def cursor_field(self) -> str:
        """Incremental cursor — the date the activity took place."""
        return "activity_date"

    # ------------------------------------------------------------------
    # JSON Schema — describes one record for the CATALOG message
    # ------------------------------------------------------------------

    def get_json_schema(self) -> Dict[str, Any]:
        """Return the JSON Schema (Draft-07) for one activity record.

        Every field is nullable because Garmin may omit any field depending on
        the device, sport type, or accessory (e.g. no HR monitor → null HR).

        Returns:
            A dict with {"type": "object", "properties": {...}}.
        """
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "activity_id":       {"type": ["integer", "null"]},
                "activity_name":     {"type": ["string",  "null"]},
                "activity_date":     {"type": ["string",  "null"], "format": "date"},
                "activity_type":     {"type": ["string",  "null"]},
                "distance_km":       {"type": ["number",  "null"]},
                "duration_minutes":  {"type": ["number",  "null"]},
                "avg_pace_min_km":   {"type": ["number",  "null"]},
                "avg_heart_rate":    {"type": ["integer", "null"]},
                "max_heart_rate":    {"type": ["integer", "null"]},
                "elevation_gain_m":  {"type": ["number",  "null"]},
                "calories":          {"type": ["integer", "null"]},
                "avg_cadence":       {"type": ["number",  "null"]},
                "event_type":        {"type": ["string",  "null"]},
                "training_effect":   {"type": ["number",  "null"]},
                "vo2max_estimate":   {"type": ["number",  "null"]},
                "ingested_at":       {"type": ["string",  "null"]},
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
        """Fetch activities from the Garmin API and yield cleaned records.

        Converts the raw list of activity dicts into a pandas DataFrame,
        applies unit conversions and sanity checks column-by-column, then
        yields one dict per row. Missing or aberrant values become None.

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
            "Fetching activities from Garmin API: {} → {}",
            start_date,
            end_date,
        )

        try:
            raw_activities: List[Dict[str, Any]] = retry_on_429(
                lambda: client.get_activities_by_date(
                    start_date.isoformat(),
                    end_date.isoformat(),
                )
            )
        except Exception as exc:
            logger.error("Garmin API error while fetching activities: {}", exc)
            raise

        if not raw_activities:
            logger.info("No activities returned for window {} → {}.", start_date, end_date)
            return

        logger.info("Retrieved {} raw activit(ies) from Garmin API.", len(raw_activities))

        # Flatten nested dicts (activityType, eventType) before loading into
        # pandas — DataFrames work best with scalar values, not nested objects.
        normalized = [self._normalize_raw(r) for r in raw_activities]

        df = pd.DataFrame(normalized)
        df = self._transform(df)

        # Select only the output columns in schema order.
        output_columns = [
            "activity_id", "activity_name", "activity_date", "activity_type",
            "distance_km", "duration_minutes", "avg_pace_min_km",
            "avg_heart_rate", "max_heart_rate", "elevation_gain_m",
            "calories", "avg_cadence", "event_type",
            "training_effect", "vo2max_estimate",
        ]
        df = df[output_columns]

        # Yield one dict per row. pandas uses NaN/NaT for missing values;
        # we replace them with None so the records serialise cleanly to JSON.
        for _, row in df.iterrows():
            yield row.where(pd.notna(row), other=None).to_dict()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _normalize_raw(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten one raw Garmin activity dict into a simple key→value dict.

        Extracts nested objects (activityType, eventType) into scalar fields so
        the whole activity can be loaded into a pandas row without object columns.

        Args:
            raw: A single activity dict from get_activities_by_date().

        Returns:
            A flat dict with string/number/None values only.
        """
        activity_type = raw.get("activityType") or {}
        event_type = raw.get("eventType") or {}

        return {
            "activity_id":         raw.get("activityId"),
            "activity_name":       raw.get("activityName"),
            "start_time_local":    raw.get("startTimeLocal"),
            "activity_type":       activity_type.get("typeKey"),
            "event_type":          event_type.get("typeKey"),
            "distance_m":          raw.get("distance"),
            "duration_s":          raw.get("duration"),
            "avg_speed_ms":        raw.get("averageSpeed"),
            "avg_hr_raw":          raw.get("averageHR"),
            "max_hr_raw":          raw.get("maxHR"),
            "elevation_gain_m":    raw.get("elevationGain"),
            "calories_raw":        raw.get("calories"),
            "avg_cadence":         raw.get("averageRunningCadenceInStepsPerMinute"),
            "training_effect_raw": raw.get("aerobicTrainingEffect"),
            "vo2max_raw":          raw.get("vO2MaxValue"),
        }

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply unit conversions and sanity checks to the raw DataFrame.

        Each transformation is a separate pandas column operation so that a
        failure in one column never blocks the others.

        Args:
            df: Raw normalized DataFrame from _normalize_raw().

        Returns:
            A cleaned DataFrame with output column names.
        """
        # ── Date parsing ────────────────────────────────────────────────
        # startTimeLocal format: "2024-01-15 09:30:00"
        # errors="coerce" turns unparseable values into NaT instead of raising.
        df["activity_date"] = (
            pd.to_datetime(df["start_time_local"], errors="coerce")
            .dt.date
            .apply(lambda d: d.isoformat() if pd.notna(d) else None)
        )

        # ── Unit conversions ────────────────────────────────────────────
        # Distance: metres → km, rounded to 3 decimal places (1 m precision).
        df["distance_km"] = df["distance_m"].apply(
            lambda v: round(float(v) / 1000, 3) if pd.notna(v) and float(v) >= 0 else None
        )

        # Duration: seconds → minutes, rounded to 2 decimal places.
        df["duration_minutes"] = df["duration_s"].apply(
            lambda v: round(float(v) / 60, 2) if pd.notna(v) and float(v) >= 0 else None
        )

        # Pace: m/s → min/km.  Wrapped in a method for the sanity-check logic.
        df["avg_pace_min_km"] = df["avg_speed_ms"].apply(self._speed_to_pace)

        # ── Heart rate sanity checks ─────────────────────────────────────
        # Values of 0 mean "no HR monitor" — treat as None.
        df["avg_heart_rate"] = df["avg_hr_raw"].apply(
            lambda v: self._check_hr(v, "avg_heart_rate")
        )
        df["max_heart_rate"] = df["max_hr_raw"].apply(
            lambda v: self._check_hr(v, "max_heart_rate")
        )

        # ── Calories (integer) ──────────────────────────────────────────
        df["calories"] = df["calories_raw"].apply(
            lambda v: int(v) if pd.notna(v) and v >= 0 else None
        )

        # ── Training effect (0.0–5.0) ───────────────────────────────────
        df["training_effect"] = df["training_effect_raw"].apply(
            lambda v: self._check_range(
                v, _MIN_TRAINING_EFFECT, _MAX_TRAINING_EFFECT, "training_effect"
            )
        )

        # ── VO2max (5–100) ───────────────────────────────────────────────
        df["vo2max_estimate"] = df["vo2max_raw"].apply(
            lambda v: self._check_range(v, _MIN_VO2MAX, _MAX_VO2MAX, "vo2max_estimate")
        )

        # ── Integer casting ──────────────────────────────────────────────
        # pandas stores integer columns with NaN as float64 by default.
        # pd.Int64Dtype() (nullable integer) preserves int type alongside NA,
        # so activity_id=12345678 stays 12345678 (not 12345678.0) in the output.
        for col in ["activity_id", "avg_heart_rate", "max_heart_rate", "calories"]:
            if col in df.columns:
                df[col] = df[col].astype(pd.Int64Dtype())

        return df

    def _speed_to_pace(self, speed_ms: Any) -> Optional[float]:
        """Convert average speed (m/s) to pace (min/km) with sanity checks.

        A pace > 30 min/km means the athlete was essentially stationary;
        a speed > 13 m/s (~47 km/h) is beyond any human running performance.
        Both are treated as sensor errors.

        Args:
            speed_ms: Raw speed value in metres per second, or None/NaN.

        Returns:
            Pace in minutes per kilometre, rounded to 2 decimal places,
            or None if the value is missing or implausible.
        """
        if pd.isna(speed_ms) or speed_ms is None:
            return None
        try:
            speed = float(speed_ms)
        except (TypeError, ValueError):
            return None

        if speed <= _MIN_SPEED_MS or speed > _MAX_SPEED_MS:
            if speed > _MAX_SPEED_MS:
                logger.warning(
                    "Aberrant averageSpeed {} m/s (>{} m/s) — setting avg_pace_min_km to None.",
                    speed,
                    _MAX_SPEED_MS,
                )
            return None

        pace = (1000.0 / speed) / 60.0  # min/km

        if pace > _MAX_PACE_MIN_KM:
            return None  # too slow to be a meaningful activity

        return round(pace, 2)

    def _check_hr(self, value: Any, field_name: str) -> Optional[int]:
        """Validate a heart rate value is within physiological bounds.

        Garmin devices record 0 bpm when no HR monitor is connected, and
        occasionally produce spikes far above the maximum human HR.

        Args:
            value: Raw HR value from the API, or None/NaN.
            field_name: Field name used in the warning log message.

        Returns:
            HR as an integer if plausible, or None otherwise.
        """
        if pd.isna(value) or value is None or value == 0:
            return None
        try:
            hr = int(value)
        except (TypeError, ValueError):
            return None

        if not (_MIN_HR_BPM <= hr <= _MAX_HR_BPM):
            logger.warning(
                "Aberrant {} value {} bpm (expected {}-{}) — setting to None.",
                field_name,
                hr,
                _MIN_HR_BPM,
                _MAX_HR_BPM,
            )
            return None

        return hr

    def _check_range(
        self,
        value: Any,
        min_val: float,
        max_val: float,
        field_name: str,
    ) -> Optional[float]:
        """Validate a numeric value is within an expected range.

        Generic version of _check_hr used for training_effect and vo2max_estimate.

        Args:
            value: Raw numeric value, or None/NaN.
            min_val: Inclusive lower bound.
            max_val: Inclusive upper bound.
            field_name: Field name used in the warning log message.

        Returns:
            The value as a float if within range, or None otherwise.
        """
        if pd.isna(value) or value is None:
            return None
        try:
            v = float(value)
        except (TypeError, ValueError):
            return None

        if not (min_val <= v <= max_val):
            logger.warning(
                "Aberrant {} value {} (expected {}-{}) — setting to None.",
                field_name,
                v,
                min_val,
                max_val,
            )
            return None

        return v
