"""
SourceGarmin — top-level orchestrator for the Garmin Connect source connector.

This module wires together config, auth, and streams to implement the three
Airbyte read commands:

  python main.py check   --config /secrets/config.json
  python main.py discover --config /secrets/config.json
  python main.py read    --config /secrets/config.json --catalog /secrets/catalog.json

Each public method returns or yields plain Python dicts shaped as Airbyte
protocol messages.  main.py is solely responsible for serialising them to stdout.
"""

import json
from typing import Any, Dict, Iterator, List, Optional

import garminconnect
from loguru import logger

from source_garmin.auth import GarminAuth
from source_garmin.config import ConnectorConfig, load_config
from source_garmin.streams.activities import ActivitiesStream
from source_garmin.streams.base import GarminStream
from source_garmin.streams.calendar_events import CalendarEventsStream
from source_garmin.streams.daily_health import DailyHealthStream


class SourceGarmin:
    """Orchestrates check, discover, and read operations for the connector.

    Responsibilities:
      - Load and validate the config file
      - Authenticate once and share the client across all streams
      - Build CATALOG / CONNECTION_STATUS messages from stream metadata
      - Drive the read loop for each stream listed in the configured catalog

    Registered streams:
      - activities      (FULL_REFRESH + INCREMENTAL, cursor: activity_date)
      - daily_health    (FULL_REFRESH + INCREMENTAL, cursor: date)
      - calendar_events (FULL_REFRESH only)
    """

    def streams(self) -> List[GarminStream]:
        """Return one instance of every available stream.

        This is the single place where streams are registered. Adding a new
        stream only requires importing it and appending it here.

        Returns:
            A list of GarminStream instances, one per stream type.
        """
        return [
            ActivitiesStream(),
            DailyHealthStream(),
            CalendarEventsStream(),
        ]

    # ------------------------------------------------------------------
    # check
    # ------------------------------------------------------------------

    def check(self, config_path: str) -> Dict[str, Any]:
        """Validate credentials by attempting a real (but lightweight) API call.

        Loads the config, authenticates via GarminAuth (which handles session
        restore and retry logic), then calls get_full_name() as the lightest
        available authenticated endpoint.

        Args:
            config_path: Path to the Airbyte config JSON file.

        Returns:
            An Airbyte CONNECTION_STATUS message dict with status SUCCEEDED or
            FAILED and a human-readable message.
        """
        try:
            config = load_config(config_path)
        except Exception as exc:
            return self._connection_status("FAILED", f"Invalid config: {exc}")

        try:
            auth = GarminAuth(config)
            client = auth.get_client()
            # get_full_name() is the lightest authenticated call available —
            # it confirms the session is valid without fetching heavy data.
            name = client.get_full_name()
            logger.info("Connection check succeeded for Garmin account: {}", name)
            return self._connection_status("SUCCEEDED", f"Connected as {name}")

        except garminconnect.GarminConnectAuthenticationError as exc:
            logger.error("Connection check failed — authentication error: {}", exc)
            return self._connection_status(
                "FAILED", "Authentication failed: invalid email or password."
            )
        except garminconnect.GarminConnectTooManyRequestsError as exc:
            logger.error("Connection check failed — rate limited: {}", exc)
            return self._connection_status(
                "FAILED",
                "Garmin rate-limited the login attempt. Wait a few minutes and retry.",
            )
        except Exception as exc:
            logger.error("Connection check failed — unexpected error: {}", exc)
            return self._connection_status("FAILED", str(exc))

    # ------------------------------------------------------------------
    # discover
    # ------------------------------------------------------------------

    def discover(self, config_path: str) -> Dict[str, Any]:
        """Build the CATALOG message from stream metadata.

        Does not need a live Garmin connection — schemas are defined statically
        in each stream class.

        Args:
            config_path: Path to the Airbyte config JSON file (validated but
                not used for API calls here).

        Returns:
            An Airbyte CATALOG message dict listing all available streams with
            their schemas and supported sync modes.
        """
        try:
            load_config(config_path)  # validate config even though we don't use it
        except Exception as exc:
            logger.warning("Config validation failed during discover: {}", exc)

        catalog_streams = [stream.get_catalog_entry() for stream in self.streams()]

        logger.info("Discovered {} stream(s).", len(catalog_streams))

        return {
            "type": "CATALOG",
            "catalog": {
                "streams": catalog_streams,
            },
        }

    # ------------------------------------------------------------------
    # read
    # ------------------------------------------------------------------

    def read(
        self,
        config_path: str,
        catalog_path: str,
        state_path: Optional[str] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Yield RECORD and STATE messages for every stream in the catalog.

        Authenticates once, then iterates over the streams listed in the
        configured catalog. Each stream drives its own read loop via
        GarminStream.read(), yielding records and a final STATE message.

        Args:
            config_path: Path to the Airbyte config JSON file.
            catalog_path: Path to the configured catalog JSON file, which
                lists the streams to sync and their sync modes.
            state_path: Optional path to the state JSON file from a previous
                run. If absent, all incremental streams start from scratch.

        Yields:
            Airbyte RECORD message dicts (one per activity / health record),
            followed by a STATE message dict at the end of each stream.
        """
        config = load_config(config_path)
        catalog = self._load_json(catalog_path)
        state = self._load_state(state_path)

        # Authenticate once — the client is shared across all streams so we
        # never trigger more than one SSO login per connector run.
        auth = GarminAuth(config)
        client = auth.get_client()

        # Build a lookup: stream name → configured sync mode.
        # Only streams listed in the catalog are synced; the rest are skipped.
        configured: Dict[str, str] = {
            entry["stream"]["name"]: entry["sync_mode"]
            for entry in catalog.get("streams", [])
        }

        available: Dict[str, GarminStream] = {
            stream.name: stream for stream in self.streams()
        }

        for stream_name, sync_mode in configured.items():
            if stream_name not in available:
                logger.warning(
                    "Stream '{}' is in the catalog but not implemented — skipping.",
                    stream_name,
                )
                continue

            stream = available[stream_name]
            # Extract only this stream's portion of the global state dict.
            stream_state: Dict[str, Any] = state.get(stream_name, {})

            logger.info(
                "Starting stream '{}' | sync_mode={} | state={}",
                stream_name,
                sync_mode,
                stream_state,
            )

            yield from stream.read(client, config, sync_mode, stream_state)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _connection_status(status: str, message: str) -> Dict[str, Any]:
        """Build an Airbyte CONNECTION_STATUS message.

        Args:
            status: 'SUCCEEDED' or 'FAILED'.
            message: Human-readable description shown in the Airbyte UI.

        Returns:
            A dict shaped as an Airbyte AirbyteConnectionStatus message.
        """
        return {
            "type": "CONNECTION_STATUS",
            "connectionStatus": {
                "status": status,
                "message": message,
            },
        }

    @staticmethod
    def _load_json(path: str) -> Dict[str, Any]:
        """Load and parse a JSON file.

        Args:
            path: Absolute or relative path to the JSON file.

        Returns:
            Parsed dict.

        Raises:
            FileNotFoundError: If the file does not exist.
            json.JSONDecodeError: If the file is not valid JSON.
        """
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    @staticmethod
    def _load_state(state_path: Optional[str]) -> Dict[str, Any]:
        """Load the state file, returning an empty dict if absent or unreadable.

        Airbyte passes state as a file containing either:
          a) The raw state data dict: {"activities": {"activity_date": "2024-01-15"}}
          b) A full STATE message:    {"type": "STATE", "state": {"data": {...}}}

        Both formats are handled gracefully.

        Args:
            state_path: Path to the state JSON file, or None.

        Returns:
            A dict mapping stream names to their last saved cursor dicts.
            Returns {} on any failure so the connector always starts cleanly.
        """
        if not state_path:
            return {}

        try:
            with open(state_path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)

            # Handle the wrapped STATE message format.
            if raw.get("type") == "STATE":
                return raw.get("state", {}).get("data", {})

            return raw

        except Exception as exc:
            logger.warning(
                "Could not load state from {} ({}). Starting fresh.",
                state_path,
                exc,
            )
            return {}
