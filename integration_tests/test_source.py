"""
Integration tests for airbyte-source-garmin.

These tests call the live Garmin Connect API and require real credentials.

Setup:
    cp integration_tests/sample_files/config.json secrets/config.json
    # edit secrets/config.json with your real Garmin email and password
    pytest integration_tests/ -v -s

The tests are skipped automatically when secrets/config.json is absent or
still contains placeholder credentials.  They are NOT designed for CI without
credential management (e.g. GitHub Actions secrets).

Session note:
    config_path is session-scoped so Garmin logs in once and all subsequent
    tests reuse the cached OAuth token via _try_load_session() — this avoids
    rate-limiting from repeated SSO logins.
"""

import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import pytest

from source_garmin.source import SourceGarmin

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).parent.parent
_SECRETS_CONFIG = _PROJECT_ROOT / "secrets" / "config.json"


# ---------------------------------------------------------------------------
# Skip guard
# ---------------------------------------------------------------------------

def _secrets_are_available() -> bool:
    """Return True only when secrets/config.json has real (non-placeholder) credentials."""
    if not _SECRETS_CONFIG.exists():
        return False
    try:
        data = json.loads(_SECRETS_CONFIG.read_text())
        email = data.get("email", "")
        password = data.get("password", "")
        return (
            bool(email)
            and not email.endswith("@example.com")
            and bool(password)
            and password != "your-garmin-password"
        )
    except Exception:
        return False


requires_secrets = pytest.mark.skipif(
    not _secrets_are_available(),
    reason=(
        "secrets/config.json not found or still contains placeholder credentials. "
        "Copy integration_tests/sample_files/config.json → secrets/config.json "
        "and fill in your real Garmin credentials."
    ),
)


# ---------------------------------------------------------------------------
# Session-scoped config fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def config_path(tmp_path_factory) -> str:
    """Write a test-specific config with lookback_days=30 and return its path.

    Session-scoped so the same config (and session file path) is shared by all
    tests.  The shared session_file_path means Garmin authenticates once —
    subsequent tests restore the OAuth token from disk rather than re-logging in.
    """
    secrets = json.loads(_SECRETS_CONFIG.read_text())
    test_config = {
        "email": secrets["email"],
        "password": secrets["password"],
        "lookback_days": 30,
        "session_file_path": str(
            tmp_path_factory.mktemp("session") / "garmin_session.json"
        ),
    }
    config_file = tmp_path_factory.mktemp("config") / "config.json"
    config_file.write_text(json.dumps(test_config))
    return str(config_file)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_catalog(tmp_path: Path, streams: List[Dict[str, Any]]) -> str:
    """Write a configured catalog JSON and return its path."""
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps({"streams": streams}))
    return str(path)


def _write_state(tmp_path: Path, data: Dict[str, Any], filename: str = "state.json") -> str:
    """Write a state JSON and return its path."""
    path = tmp_path / filename
    path.write_text(json.dumps(data))
    return str(path)


def _messages_of_type(messages: List[Dict], msg_type: str) -> List[Dict]:
    """Filter a list of Airbyte messages by their type field."""
    return [m for m in messages if m.get("type") == msg_type]


# ---------------------------------------------------------------------------
# Catalog entry constants (mirrors integration_tests/sample_files/configured_catalog.json)
# ---------------------------------------------------------------------------

_ACTIVITIES_ENTRY = {
    "stream": {
        "name": "activities",
        "json_schema": {},
        "supported_sync_modes": ["full_refresh", "incremental"],
        "source_defined_cursor": True,
        "default_cursor_field": ["activity_date"],
    },
    "sync_mode": "incremental",
    "cursor_field": ["activity_date"],
    "destination_sync_mode": "append",
}

_DAILY_HEALTH_ENTRY = {
    "stream": {
        "name": "daily_health",
        "json_schema": {},
        "supported_sync_modes": ["full_refresh", "incremental"],
        "source_defined_cursor": True,
        "default_cursor_field": ["date"],
    },
    "sync_mode": "incremental",
    "cursor_field": ["date"],
    "destination_sync_mode": "append",
}

_CALENDAR_EVENTS_ENTRY = {
    "stream": {
        "name": "calendar_events",
        "json_schema": {},
        "supported_sync_modes": ["full_refresh"],
        "source_defined_cursor": False,
        "default_cursor_field": [],
    },
    "sync_mode": "full_refresh",
    "cursor_field": [],
    "destination_sync_mode": "overwrite",
}


# ---------------------------------------------------------------------------
# TestCheck
# ---------------------------------------------------------------------------

@requires_secrets
class TestCheck:
    """check() validates credentials via a real Garmin API call."""

    def test_succeeds_with_valid_credentials(self, config_path):
        """CONNECTION_STATUS is SUCCEEDED when credentials are correct."""
        result = SourceGarmin().check(config_path)

        assert result["type"] == "CONNECTION_STATUS"
        assert result["connectionStatus"]["status"] == "SUCCEEDED", (
            f"Expected SUCCEEDED, got: {result['connectionStatus'].get('message')}"
        )

    def test_message_contains_account_name(self, config_path):
        """The success message includes the Garmin account display name."""
        result = SourceGarmin().check(config_path)
        message = result["connectionStatus"]["message"]
        assert message.startswith("Connected as "), f"Unexpected message: {message}"


# ---------------------------------------------------------------------------
# TestDiscover
# ---------------------------------------------------------------------------

@requires_secrets
class TestDiscover:
    """discover() returns a valid CATALOG listing all three streams."""

    _EXPECTED_STREAMS = {"activities", "daily_health", "calendar_events"}

    def test_returns_catalog_message(self, config_path):
        """Returns a dict with type == CATALOG."""
        result = SourceGarmin().discover(config_path)
        assert result["type"] == "CATALOG"

    def test_all_three_streams_present(self, config_path):
        """Catalog contains exactly the three expected streams."""
        result = SourceGarmin().discover(config_path)
        # get_catalog_entry() returns stream dicts directly (no "stream" wrapper).
        names = {s["name"] for s in result["catalog"]["streams"]}
        assert names == self._EXPECTED_STREAMS

    def test_activities_supports_incremental(self, config_path):
        """activities stream declares incremental sync mode."""
        result = SourceGarmin().discover(config_path)
        activities = next(
            s for s in result["catalog"]["streams"] if s["name"] == "activities"
        )
        assert "incremental" in activities["supported_sync_modes"]
        assert activities["source_defined_cursor"] is True

    def test_calendar_events_full_refresh_only(self, config_path):
        """calendar_events stream supports full_refresh only, no cursor."""
        result = SourceGarmin().discover(config_path)
        cal = next(
            s for s in result["catalog"]["streams"] if s["name"] == "calendar_events"
        )
        assert cal["supported_sync_modes"] == ["full_refresh"]
        assert cal["source_defined_cursor"] is False


# ---------------------------------------------------------------------------
# TestReadActivities
# ---------------------------------------------------------------------------

@requires_secrets
class TestReadActivities:
    """read() fetches real activities and emits correct Airbyte messages."""

    _REQUIRED_FIELDS = {
        "activity_id", "activity_name", "activity_date", "activity_type",
        "distance_km", "duration_minutes", "avg_pace_min_km",
        "avg_heart_rate", "max_heart_rate", "elevation_gain_m",
        "calories", "avg_cadence", "event_type", "training_effect",
        "vo2max_estimate", "ingested_at",
    }

    def test_returns_at_least_one_record(self, config_path, tmp_path):
        """At least one RECORD is emitted for a 30-day window."""
        catalog = _write_catalog(tmp_path, [_ACTIVITIES_ENTRY])
        messages = list(SourceGarmin().read(config_path, catalog))
        records = _messages_of_type(messages, "RECORD")
        assert len(records) >= 1, (
            "Expected at least one activity record in the last 30 days. "
            "If you have not used Garmin in 30 days, increase lookback_days in secrets/config.json."
        )

    def test_record_contains_all_schema_fields(self, config_path, tmp_path):
        """Every RECORD message contains all fields defined in the JSON schema."""
        catalog = _write_catalog(tmp_path, [_ACTIVITIES_ENTRY])
        messages = list(SourceGarmin().read(config_path, catalog))
        records = _messages_of_type(messages, "RECORD")
        assert records, "No records to inspect"

        for msg in records:
            data = msg["record"]["data"]
            missing = self._REQUIRED_FIELDS - data.keys()
            assert not missing, f"Record missing fields: {missing}"

    def test_activity_id_is_integer(self, config_path, tmp_path):
        """activity_id is an integer (not a float like 12345678.0)."""
        catalog = _write_catalog(tmp_path, [_ACTIVITIES_ENTRY])
        messages = list(SourceGarmin().read(config_path, catalog))
        records = _messages_of_type(messages, "RECORD")
        assert records, "No records to inspect"

        for msg in records:
            aid = msg["record"]["data"]["activity_id"]
            if aid is not None:
                assert isinstance(aid, int), f"activity_id is {type(aid)}, expected int"

    def test_state_emitted_at_end_of_incremental_sync(self, config_path, tmp_path):
        """Incremental sync emits exactly one STATE message."""
        catalog = _write_catalog(tmp_path, [_ACTIVITIES_ENTRY])
        messages = list(SourceGarmin().read(config_path, catalog))
        state_msgs = _messages_of_type(messages, "STATE")
        assert len(state_msgs) == 1, f"Expected 1 STATE message, got {len(state_msgs)}"

    def test_state_cursor_is_valid_iso_date(self, config_path, tmp_path):
        """The STATE cursor value is a parseable ISO-8601 date string."""
        catalog = _write_catalog(tmp_path, [_ACTIVITIES_ENTRY])
        messages = list(SourceGarmin().read(config_path, catalog))
        state_msgs = _messages_of_type(messages, "STATE")
        assert state_msgs, "No STATE message emitted"

        cursor = state_msgs[0]["state"]["data"]["activities"]["activity_date"]
        date.fromisoformat(cursor)  # raises ValueError if not a valid date


# ---------------------------------------------------------------------------
# TestReadDailyHealth
# ---------------------------------------------------------------------------

@requires_secrets
class TestReadDailyHealth:
    """read() fetches real daily health summaries."""

    _REQUIRED_FIELDS = {
        "date", "steps", "resting_heart_rate", "hrv_avg",
        "sleep_seconds", "deep_sleep_seconds", "stress_avg",
        "body_battery_charged", "body_battery_drained",
        "active_calories", "ingested_at",
    }

    def test_returns_at_least_one_record(self, config_path, tmp_path):
        """At least one RECORD is emitted for a 30-day window."""
        catalog = _write_catalog(tmp_path, [_DAILY_HEALTH_ENTRY])
        messages = list(SourceGarmin().read(config_path, catalog))
        records = _messages_of_type(messages, "RECORD")
        assert len(records) >= 1, "Expected at least one daily health record in the last 30 days"

    def test_record_contains_all_schema_fields(self, config_path, tmp_path):
        """Every RECORD contains all fields defined in the JSON schema."""
        catalog = _write_catalog(tmp_path, [_DAILY_HEALTH_ENTRY])
        messages = list(SourceGarmin().read(config_path, catalog))
        records = _messages_of_type(messages, "RECORD")
        assert records, "No records to inspect"

        for msg in records:
            missing = self._REQUIRED_FIELDS - msg["record"]["data"].keys()
            assert not missing, f"Record missing fields: {missing}"

    def test_state_cursor_equals_max_date_in_records(self, config_path, tmp_path):
        """The STATE cursor equals the most recent date in the returned records."""
        catalog = _write_catalog(tmp_path, [_DAILY_HEALTH_ENTRY])
        messages = list(SourceGarmin().read(config_path, catalog))
        records = _messages_of_type(messages, "RECORD")
        state_msgs = _messages_of_type(messages, "STATE")

        assert records, "No records to compare cursor against"
        assert state_msgs, "No STATE message emitted"

        cursor = state_msgs[0]["state"]["data"]["daily_health"]["date"]
        max_date = max(msg["record"]["data"]["date"] for msg in records)
        assert cursor == max_date, (
            f"STATE cursor {cursor!r} does not match max record date {max_date!r}"
        )


# ---------------------------------------------------------------------------
# TestReadCalendarEvents
# ---------------------------------------------------------------------------

@requires_secrets
class TestReadCalendarEvents:
    """read() fetches calendar events — FULL_REFRESH only, no STATE emitted."""

    _REQUIRED_FIELDS = {
        "event_id", "event_title", "event_date", "event_type",
        "distance_km", "location", "url", "ingested_at",
    }

    def test_runs_without_error(self, config_path, tmp_path):
        """Sync completes without raising even when there are no upcoming events."""
        catalog = _write_catalog(tmp_path, [_CALENDAR_EVENTS_ENTRY])
        messages = list(SourceGarmin().read(config_path, catalog))
        # Record count is not asserted — some users have no calendar events.
        assert isinstance(messages, list)

    def test_no_state_message_emitted(self, config_path, tmp_path):
        """FULL_REFRESH stream never emits a STATE message."""
        catalog = _write_catalog(tmp_path, [_CALENDAR_EVENTS_ENTRY])
        messages = list(SourceGarmin().read(config_path, catalog))
        state_msgs = _messages_of_type(messages, "STATE")
        assert state_msgs == [], f"Expected no STATE messages, got {len(state_msgs)}"

    def test_records_have_required_fields_when_present(self, config_path, tmp_path):
        """Any returned event record contains all schema-defined fields."""
        catalog = _write_catalog(tmp_path, [_CALENDAR_EVENTS_ENTRY])
        messages = list(SourceGarmin().read(config_path, catalog))
        records = _messages_of_type(messages, "RECORD")

        for msg in records:
            missing = self._REQUIRED_FIELDS - msg["record"]["data"].keys()
            assert not missing, f"Event record missing fields: {missing}"


# ---------------------------------------------------------------------------
# TestIncrementalResume
# ---------------------------------------------------------------------------

@requires_secrets
class TestIncrementalResume:
    """Verify that the incremental cursor is respected on subsequent syncs."""

    def test_future_cursor_yields_no_records(self, config_path, tmp_path):
        """When the saved cursor is in the far future, 0 records are returned.

        _compute_start_date() uses the cursor as start_date.  If start_date is
        2099-12-31 and end_date is today, the date window is empty and no API
        call is made.  This verifies the cursor is honoured end-to-end.
        """
        state_path = _write_state(
            tmp_path,
            {"activities": {"activity_date": "2099-12-31"}},
        )
        catalog = _write_catalog(tmp_path, [_ACTIVITIES_ENTRY])
        messages = list(SourceGarmin().read(config_path, catalog, state_path))
        records = _messages_of_type(messages, "RECORD")

        assert records == [], (
            f"Expected 0 records with a future cursor but got {len(records)}"
        )

    def test_second_run_returns_no_more_records_than_first(self, config_path, tmp_path):
        """Run 2 (starting from run 1's cursor) returns ≤ records than run 1.

        Concretely: the two runs happen milliseconds apart, so run 2 should
        return 0 new records.  The assertion is ≤ rather than == 0 to avoid
        flakiness if a Garmin sync arrives between the two calls.
        """
        catalog = _write_catalog(tmp_path, [_ACTIVITIES_ENTRY])

        # Run 1 — no saved state, full 30-day window.
        run1_messages = list(SourceGarmin().read(config_path, catalog))
        run1_states = _messages_of_type(run1_messages, "STATE")
        assert run1_states, "Run 1 must emit a STATE message to continue"

        # Save run 1's cursor as run 2's starting state.
        state_path = _write_state(
            tmp_path,
            run1_states[0]["state"]["data"],
            filename="state_run2.json",
        )

        # Run 2 — incremental from run 1's cursor.
        run2_messages = list(SourceGarmin().read(config_path, catalog, state_path))
        run2_records = _messages_of_type(run2_messages, "RECORD")
        run1_records = _messages_of_type(run1_messages, "RECORD")

        assert len(run2_records) <= len(run1_records), (
            f"Run 2 returned {len(run2_records)} records, "
            f"more than run 1's {len(run1_records)}"
        )
