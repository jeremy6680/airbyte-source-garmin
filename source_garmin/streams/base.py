"""
Abstract base class for all Garmin Connect streams.

Every stream (activities, daily_health, calendar_events) must subclass
GarminStream and implement the abstract properties and methods defined here.
The base class handles:
  - Airbyte protocol message formatting (RECORD, STATE)
  - Incremental cursor tracking and state management
  - Date-range calculation from config.lookback_days and stream state
  - Automatic injection of the `ingested_at` field into every record
"""

from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional

import garminconnect
from loguru import logger

from source_garmin.config import ConnectorConfig


class GarminStream(ABC):
    """Abstract base class that all Garmin Connect streams must extend.

    Subclasses must implement:
      - name (property)
      - primary_key (property)
      - get_json_schema()
      - read_records()

    Subclasses may optionally override:
      - cursor_field (property) — set to a field name to enable INCREMENTAL mode
      - supported_sync_modes (property) — defaults to full_refresh only, or
        full_refresh + incremental when cursor_field is set
    """

    # ------------------------------------------------------------------
    # Abstract interface — subclasses MUST implement these
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def name(self) -> str:
        """Stream name as it appears in the Airbyte catalog (e.g. 'activities')."""

    @property
    @abstractmethod
    def primary_key(self) -> str:
        """Name of the field that uniquely identifies a record (e.g. 'activity_id')."""

    @abstractmethod
    def get_json_schema(self) -> Dict[str, Any]:
        """Return the JSON Schema (Draft 7) that describes one record of this stream.

        Returns:
            A dict with at minimum {"type": "object", "properties": {...}}.
        """

    @abstractmethod
    def read_records(
        self,
        client: garminconnect.Garmin,
        config: ConnectorConfig,
        start_date: date,
        end_date: date,
    ) -> Iterator[Dict[str, Any]]:
        """Fetch raw records from the Garmin API for the given date window.

        The base class computes start_date and end_date from config and state
        before calling this method — implementations only need to fetch and
        yield flat record dicts. Do NOT add `ingested_at` here; the base class
        injects it automatically.

        Args:
            client: An authenticated garminconnect.Garmin instance.
            config: Validated connector configuration.
            start_date: Inclusive lower bound of the date window.
            end_date: Inclusive upper bound (today).

        Yields:
            Flat dicts representing individual records. Missing or aberrant
            fields should be set to None rather than raising.
        """

    # ------------------------------------------------------------------
    # Optional overrides — subclasses MAY override these
    # ------------------------------------------------------------------

    @property
    def cursor_field(self) -> Optional[str]:
        """Field name used as the incremental cursor.

        Returns None by default, meaning the stream supports FULL_REFRESH only.
        Override in subclasses that support INCREMENTAL sync.

        Returns:
            A field name string (e.g. 'activity_date'), or None.
        """
        return None

    @property
    def supported_sync_modes(self) -> List[str]:
        """Sync modes advertised in the CATALOG message.

        Automatically includes 'incremental' when cursor_field is set.

        Returns:
            A list containing 'full_refresh', and optionally 'incremental'.
        """
        if self.cursor_field:
            return ["full_refresh", "incremental"]
        return ["full_refresh"]

    # ------------------------------------------------------------------
    # Catalog helper — used by source.py to build CATALOG messages
    # ------------------------------------------------------------------

    def get_catalog_entry(self) -> Dict[str, Any]:
        """Build the AirbyteStream descriptor for a CATALOG message.

        Returns:
            A dict shaped as an Airbyte AirbyteStream object, ready to be
            included in a CATALOG message's streams list.
        """
        entry: Dict[str, Any] = {
            "name": self.name,
            "json_schema": self.get_json_schema(),
            "supported_sync_modes": self.supported_sync_modes,
            # source_defined_cursor = True means Airbyte trusts the connector
            # to manage the cursor rather than the destination tracking it.
            "source_defined_cursor": self.cursor_field is not None,
            "default_cursor_field": [self.cursor_field] if self.cursor_field else [],
        }
        return entry

    # ------------------------------------------------------------------
    # Main read loop — orchestrates fetch → enrich → emit
    # ------------------------------------------------------------------

    def read(
        self,
        client: garminconnect.Garmin,
        config: ConnectorConfig,
        sync_mode: str,
        stream_state: Optional[Dict[str, Any]] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Orchestrate a full sync run for this stream.

        Computes the date window, calls read_records(), injects ingested_at,
        emits RECORD messages, tracks the incremental cursor, and emits a
        final STATE message when done.

        Args:
            client: An authenticated garminconnect.Garmin instance.
            config: Validated connector configuration.
            sync_mode: 'full_refresh' or 'incremental'.
            stream_state: The last saved state dict for this stream, e.g.
                {'activity_date': '2024-01-15'}. Pass {} or None for a
                first-time sync.

        Yields:
            Airbyte RECORD message dicts, followed by a STATE message dict
            (only emitted when cursor_field is set).
        """
        if stream_state is None:
            stream_state = {}

        today = date.today()
        start_date = self._compute_start_date(config, sync_mode, stream_state, today)

        logger.info(
            "Stream '{}' | mode={} | window {} → {}",
            self.name,
            sync_mode,
            start_date,
            today,
        )

        # latest_cursor tracks the highest cursor value seen in this run so
        # we can emit an accurate STATE message at the end.
        latest_cursor: Optional[str] = stream_state.get(self.cursor_field) if self.cursor_field else None
        record_count = 0

        for raw_record in self.read_records(client, config, start_date, today):
            enriched = self._enrich_record(raw_record)
            record_count += 1
            yield self._make_record_message(enriched)

            # Update the cursor bookmark if this record is newer than what we
            # have seen so far. String comparison works for ISO date strings.
            if self.cursor_field and enriched.get(self.cursor_field):
                cursor_value = str(enriched[self.cursor_field])
                if latest_cursor is None or cursor_value > latest_cursor:
                    latest_cursor = cursor_value

        logger.info("Stream '{}' emitted {} record(s).", self.name, record_count)

        # Emit STATE so Airbyte knows where to resume on the next incremental run.
        if self.cursor_field and latest_cursor:
            yield self._make_state_message({self.cursor_field: latest_cursor})

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _compute_start_date(
        self,
        config: ConnectorConfig,
        sync_mode: str,
        stream_state: Dict[str, Any],
        today: date,
    ) -> date:
        """Determine the start of the fetch window.

        For INCREMENTAL: resume from the last cursor value saved in state.
        For FULL_REFRESH (or when there is no saved state): look back
        config.lookback_days from today.

        Args:
            config: Validated connector configuration.
            sync_mode: 'full_refresh' or 'incremental'.
            stream_state: The saved state dict for this stream.
            today: The reference "end" date (injected so tests can override it).

        Returns:
            The start date for the fetch window.
        """
        if (
            sync_mode == "incremental"
            and self.cursor_field
            and stream_state.get(self.cursor_field)
        ):
            try:
                return date.fromisoformat(stream_state[self.cursor_field])
            except ValueError:
                logger.warning(
                    "Could not parse cursor value '{}' as a date — "
                    "falling back to lookback window.",
                    stream_state[self.cursor_field],
                )

        return today - timedelta(days=config.lookback_days)

    def _enrich_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Inject the `ingested_at` timestamp into a record dict.

        Uses UTC time with timezone-awareness (datetime.utcnow() is deprecated
        in Python 3.11+).

        Args:
            record: The raw record dict from read_records().

        Returns:
            A new dict with all original fields plus `ingested_at`.
        """
        return {
            **record,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }

    def _make_record_message(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Format an Airbyte RECORD protocol message.

        Args:
            data: The enriched record dict.

        Returns:
            A dict shaped as {"type": "RECORD", "record": {...}}.
        """
        return {
            "type": "RECORD",
            "record": {
                "stream": self.name,
                "data": data,
                # emitted_at is Unix milliseconds (Airbyte convention).
                "emitted_at": int(datetime.now(timezone.utc).timestamp() * 1000),
            },
        }

    def _make_state_message(self, state_data: Dict[str, Any]) -> Dict[str, Any]:
        """Format an Airbyte STATE protocol message.

        The state dict is namespaced under the stream name so multiple streams
        can coexist in the same top-level state object without key collisions.

        Args:
            state_data: A dict of cursor field → last seen value,
                e.g. {'activity_date': '2024-01-15'}.

        Returns:
            A dict shaped as {"type": "STATE", "state": {"data": {stream: {...}}}}.
        """
        return {
            "type": "STATE",
            "state": {
                "data": {self.name: state_data},
            },
        }
