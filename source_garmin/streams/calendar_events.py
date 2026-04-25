"""
Calendar events stream for airbyte-source-garmin.

Fetches upcoming races and events from the Garmin Connect calendar using
get_scheduled_workouts(), which returns the full calendar for a given month.
Only items with itemType == 'event' are kept — activities, naps, weight
measurements and other item types are excluded (activities are already covered
by the activities stream).

Design choices:
  - FULL_REFRESH only (no cursor) because calendar events are mutable —
    a race can be cancelled, renamed, or rescheduled between syncs.
  - Iterates month by month over [start_date, today + _FORWARD_DAYS] and
    deduplicates by URL since the API returns null 'id' for event items.
  - Garmin's API sometimes returns items from adjacent months in a given
    month's response; deduplication by URL handles this correctly.
  - event_id is a synthetic integer derived from hash(title|date) because
    the Garmin API returns null for the 'id' field on event-type items.
"""

from datetime import date, timedelta
from typing import Any, Dict, Iterator, List, Optional, Set

import garminconnect
import pandas as pd
from loguru import logger

from source_garmin.config import ConnectorConfig
from source_garmin.streams.base import GarminStream
from source_garmin.utils import retry_on_429

# How far into the future to look for upcoming calendar events.
_FORWARD_DAYS: int = 365

# Only these itemType values are actual events (races, competitions).
# 'activity' items are already in the activities stream.
_EVENT_ITEM_TYPE = "event"


def _synthetic_event_id(title: Optional[str], event_date: Optional[str]) -> int:
    """Generate a stable integer ID for a calendar event that has no real ID.

    The Garmin API returns null for the 'id' field on event-type items.
    We derive a deterministic integer from the title and date so that the
    same event produces the same ID across two consecutive syncs.

    Args:
        title: The event title string, e.g. "Nice Marathon 2026".
        event_date: The event date string, e.g. "2026-10-05".

    Returns:
        A positive integer that fits in a 32-bit signed int.
    """
    key = f"{title or ''}|{event_date or ''}"
    # Python's hash() is seeded per-process; use abs() and modulo to get a
    # stable, positive int that round-trips through JSON without overflow.
    return abs(hash(key)) % (2**31 - 1)


class CalendarEventsStream(GarminStream):
    """Stream that yields one record per Garmin calendar event (race / competition).

    Supports FULL_REFRESH only — calendar events are re-fetched in their
    entirety on every sync because past events may be edited and future
    events may be added or cancelled between runs.

    The stream calls get_scheduled_workouts(year, month) once per calendar
    month from start_date to (today + _FORWARD_DAYS). It keeps only items
    where itemType == 'event' (upcoming races sourced from the Garmin/ahotu
    catalog). A set of seen URLs prevents duplicates when the API returns
    items from adjacent months.

    Field mapping:
        Garmin raw field   → Our schema field
        synthetic hash     → event_id   (API returns null id for events)
        title              → event_title
        date               → event_date
        itemType           → event_type  (always 'event' after filtering)
        distance           → distance_km (usually null; distance is in title)
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

    # cursor_field intentionally not overridden → returns None from base class,
    # meaning supported_sync_modes = ["full_refresh"] only.

    # ------------------------------------------------------------------
    # JSON Schema
    # ------------------------------------------------------------------

    def get_json_schema(self) -> Dict[str, Any]:
        """Return the JSON Schema (Draft-07) for one calendar event record.

        distance_km is nullable because the Garmin calendar API returns null
        for distance on event-type items (the distance is embedded in the
        title text, e.g. "Nice Marathon (42.2 km)").

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
    # Core fetch
    # ------------------------------------------------------------------

    def read_records(
        self,
        client: garminconnect.Garmin,
        config: ConnectorConfig,
        start_date: date,
        end_date: date,
    ) -> Iterator[Dict[str, Any]]:
        """Fetch calendar events month by month and yield cleaned records.

        Overrides the base class end_date to always look 365 days forward
        from today so that upcoming races are included regardless of
        config.lookback_days.

        Calls get_scheduled_workouts(year, month) once per calendar month
        in the query window. Only items where itemType == 'event' are kept.
        Duplicate URLs (items returned in two adjacent month responses) are
        silently discarded.

        Args:
            client: An authenticated garminconnect.Garmin instance.
            config: Validated connector configuration (unused here).
            start_date: Inclusive start of the date window (from base class).
            end_date: Ignored — replaced by today + _FORWARD_DAYS.

        Yields:
            Flat dicts with the fields defined in get_json_schema(), excluding
            `ingested_at` (injected by the base class).
        """
        query_end = date.today() + timedelta(days=_FORWARD_DAYS)

        logger.info(
            "Fetching calendar events from Garmin API: {} → {}",
            start_date,
            query_end,
        )

        # Dedup by URL; events lack a real id but always have a stable URL.
        seen_urls: Set[Optional[str]] = set()
        raw_records: List[Dict[str, Any]] = []

        # Iterate one calendar month at a time.
        year, month = start_date.year, start_date.month
        end_year, end_month = query_end.year, query_end.month

        while (year, month) <= (end_year, end_month):
            try:
                response: Dict[str, Any] = retry_on_429(
                    lambda y=year, m=month: client.get_scheduled_workouts(y, m)
                ) or {}
                items: List[Dict[str, Any]] = response.get("calendarItems", [])
            except Exception as exc:
                logger.warning(
                    "Could not fetch calendar for {}-{:02d} — skipping: {}",
                    year,
                    month,
                    exc,
                )
                year, month = _next_month(year, month)
                continue

            for item in items:
                if item.get("itemType") != _EVENT_ITEM_TYPE:
                    continue
                url = item.get("url")
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                raw_records.append(self._normalize_raw(item))

            year, month = _next_month(year, month)

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

        The API returns null for 'id' on event-type items; we generate a
        synthetic integer ID from the title and date instead.

        Args:
            raw: A single calendarItem dict where itemType == 'event'.

        Returns:
            A flat dict with string/number/None values only.
        """
        title = raw.get("title")
        event_date = raw.get("date")
        return {
            "event_id":    _synthetic_event_id(title, event_date),
            "event_title": title,
            "event_date":  event_date,
            "event_type":  raw.get("itemType"),
            "distance_km": raw.get("distance"),
            "location":    raw.get("location"),
            "url":         raw.get("url"),
        }

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply type coercion to the normalised DataFrame.

        Args:
            df: Normalised DataFrame from _normalize_raw().

        Returns:
            A cleaned DataFrame with correct types.
        """
        df["event_id"] = df["event_id"].astype(pd.Int64Dtype())

        # distance_km: non-negative float in km, or None.
        # For event-type items, distance is typically null (embedded in title).
        df["distance_km"] = df["distance_km"].apply(
            lambda v: round(float(v), 3)
            if pd.notna(v) and v is not None and v >= 0
            else None
        )

        return df


def _next_month(year: int, month: int) -> tuple[int, int]:
    """Return the (year, month) tuple for the month following (year, month).

    Args:
        year: Current year.
        month: Current month (1-12).

    Returns:
        A (year, month) tuple advanced by one calendar month.
    """
    if month == 12:
        return year + 1, 1
    return year, month + 1
