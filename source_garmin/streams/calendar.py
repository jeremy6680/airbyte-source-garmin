"""
Calendar events stream for airbyte-source-garmin.

Fetches upcoming training events and races from the Garmin Connect calendar
for a forward-looking date window and maps them to the flat schema in CLAUDE.md.

Design choices:
  - FULL_REFRESH only (no cursor) because calendar events are mutable —
    a race can be cancelled, renamed, or rescheduled between syncs.
  - The Garmin API exposes a week-by-week calendar endpoint; this stream
    iterates over ISO weeks and deduplicates events by ID.
  - The query window extends 365 days into the future so that upcoming races
    are always captured regardless of config.lookback_days.
"""

from datetime import date, timedelta
from typing import Any, Dict, Iterator, List, Optional, Set

import garminconnect
import pandas as pd
from loguru import logger

from source_garmin.config import ConnectorConfig
from source_garmin.streams.base import GarminStream

# How far into the future to look for upcoming calendar events.
# A year is wide enough to capture most race registrations.
_FORWARD_DAYS: int = 365


class CalendarEventsStream(GarminStream):
    """Stream that yields one record per Garmin calendar event.

    Supports FULL_REFRESH only — calendar events are re-fetched in their
    entirety on every sync because past events may be edited and future
    events may be added or cancelled between runs.

    The stream iterates over ISO calendar weeks from start_date to
    (today + _FORWARD_DAYS), calling get_calendar_week() once per week.
    A set of seen event IDs prevents duplicates when an event falls on a
    week boundary and appears in two consecutive week responses.

    Field mapping:
        Garmin raw field   → Our schema field
        id                 → event_id
        title              → event_title
        date               → event_date
        eventType          → event_type
        distance           → distance_km  (already in km in the API response)
        location           → location
        url                → url
    """

    @property
    def name(self) -> str:
        """Stream name used in the Airbyte catalog."""
        return "calendar_events"

    @property
    def primary_key(self) -> str:
        """Unique identifier for a calendar event."""
        return "event_id"

    # cursor_field is intentionally not overridden → returns None from base class,
    # which means supported_sync_modes = ["full_refresh"] only.

    # ------------------------------------------------------------------
    # JSON Schema — describes one record for the CATALOG message
    # ------------------------------------------------------------------

    def get_json_schema(self) -> Dict[str, Any]:
        """Return the JSON Schema (Draft-07) for one calendar event record.

        Distance and location are nullable because Garmin allows creating
        events without specifying a distance or venue.

        Returns:
            A dict with {"type": "object", "properties": {...}}.
        """
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "event_id":    {"type": ["integer", "null"]},
                "event_title": {"type": ["string",  "null"]},
                "event_date":  {"type": ["string",  "null"], "format": "date"},
                "event_type":  {"type": ["string",  "null"]},
                "distance_km": {"type": ["number",  "null"]},
                "location":    {"type": ["string",  "null"]},
                "url":         {"type": ["string",  "null"]},
                "ingested_at": {"type": ["string",  "null"]},
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
        """Fetch calendar events week by week and yield cleaned records.

        Overrides the base class end_date to always look 365 days forward
        from today — this ensures upcoming races are included regardless of
        config.lookback_days. The start_date (today − lookback_days) is kept
        so recently-completed events are also captured.

        Calls get_calendar_week(year, week_number) once per ISO week in the
        query window. Duplicate event IDs (events on week boundaries that
        appear in two consecutive week responses) are silently discarded.

        Args:
            client: An authenticated garminconnect.Garmin instance.
            config: Validated connector configuration (unused but required).
            start_date: Inclusive start of the date window (from base class).
            end_date: Ignored — replaced by today + _FORWARD_DAYS so that
                upcoming events are always included.

        Yields:
            Flat dicts with the fields defined in get_json_schema(), excluding
            `ingested_at` (injected by the base class).
        """
        # Extend the window forward to capture upcoming races.
        query_end = date.today() + timedelta(days=_FORWARD_DAYS)

        logger.info(
            "Fetching calendar events from Garmin API: {} → {}",
            start_date,
            query_end,
        )

        seen_ids: Set[Any] = set()
        raw_records: List[Dict[str, Any]] = []
        current = start_date

        while current <= query_end:
            year, week, _ = current.isocalendar()
            try:
                week_items: List[Dict[str, Any]] = client.get_calendar_week(year, week) or []
            except Exception as exc:
                logger.warning(
                    "Could not fetch calendar for {}-W{:02d} — skipping: {}",
                    year,
                    week,
                    exc,
                )
                current += timedelta(weeks=1)
                continue

            for item in week_items:
                event_id = item.get("id")
                if event_id in seen_ids:
                    continue
                seen_ids.add(event_id)
                raw_records.append(self._normalize_raw(item))

            current += timedelta(weeks=1)

        if not raw_records:
            logger.info(
                "No calendar events found for window {} → {}.",
                start_date,
                query_end,
            )
            return

        logger.info("Retrieved {} calendar event(s).", len(raw_records))

        df = pd.DataFrame(raw_records)
        df = self._transform(df)

        output_columns = [
            "event_id", "event_title", "event_date", "event_type",
            "distance_km", "location", "url",
        ]
        df = df[output_columns]

        for _, row in df.iterrows():
            yield row.where(pd.notna(row), other=None).to_dict()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _normalize_raw(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Map one raw Garmin calendar item to our flat schema fields.

        No unit conversions are needed — the Garmin calendar API already
        returns distance in kilometres (unlike the activities API which uses
        metres).

        Args:
            raw: A single calendar item dict from get_calendar_week().

        Returns:
            A flat dict with string/number/None values only.
        """
        return {
            "event_id":    raw.get("id"),
            "event_title": raw.get("title"),
            "event_date":  raw.get("date"),
            "event_type":  raw.get("eventType"),
            "distance_km": raw.get("distance"),
            "location":    raw.get("location"),
            "url":         raw.get("url"),
        }

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply type coercion to the normalised DataFrame.

        Calendar events have no unit conversions — transformations are
        limited to ensuring correct types (integer event_id, float distance).

        Args:
            df: Normalised DataFrame from _normalize_raw().

        Returns:
            A cleaned DataFrame with correct types.
        """
        # event_id as nullable integer (avoids 12345.0 in JSON output)
        df["event_id"] = df["event_id"].astype(pd.Int64Dtype())

        # distance_km: non-negative float, or None
        df["distance_km"] = df["distance_km"].apply(
            lambda v: round(float(v), 3) if pd.notna(v) and v is not None and v >= 0 else None
        )

        return df
