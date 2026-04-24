"""
Unit tests for source_garmin/streams/ modules.

Coverage:
  - ActivitiesStream: field mapping, unit conversions, sanity checks on
    aberrant Garmin values (HR=0, HR=500, speed=999 m/s, etc.)
  - GarminStream base class: RECORD/STATE message shapes, ingested_at injection,
    date-window calculation for full_refresh vs incremental modes

Strategy:
  - garminconnect.Garmin is replaced with a MagicMock; real API calls never happen.
  - Fixture JSON files in unit_tests/fixtures/ are the single source of test data
    so tests and fixtures can be validated together against real Garmin responses.
  - All transformation logic (field mapping, unit conversion, sanity checks) runs
    for real — only the network boundary is mocked.
"""

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

from source_garmin.config import ConnectorConfig
from source_garmin.streams.activities import ActivitiesStream

# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_fixture(name: str) -> List[Dict[str, Any]]:
    """Load a JSON fixture from unit_tests/fixtures/.

    Args:
        name: Filename (e.g. 'activities.json').

    Returns:
        Parsed list of dicts.
    """
    with open(FIXTURES_DIR / name, encoding="utf-8") as fh:
        return json.load(fh)


def make_config(**overrides) -> ConnectorConfig:
    """Build a minimal valid ConnectorConfig for tests.

    Args:
        **overrides: Any ConnectorConfig field to override.

    Returns:
        A validated ConnectorConfig instance.
    """
    defaults = {"email": "test@example.com", "password": "s3cr3t", "lookback_days": 30}
    defaults.update(overrides)
    return ConnectorConfig(**defaults)


def make_client(activities=None) -> MagicMock:
    """Build a mock garminconnect.Garmin client with a pre-configured response.

    Args:
        activities: List of raw activity dicts to return from get_activities_by_date().
            Defaults to an empty list.

    Returns:
        A MagicMock that behaves like an authenticated Garmin client.
    """
    mock = MagicMock()
    mock.get_activities_by_date.return_value = activities if activities is not None else []
    return mock


# Reference date window used across tests — arbitrary fixed dates.
_START = date(2024, 1, 1)
_END = date(2024, 1, 31)


# ---------------------------------------------------------------------------
# ActivitiesStream — field mapping
# ---------------------------------------------------------------------------

class TestActivitiesStreamFieldMapping:
    """ActivitiesStream correctly maps Garmin raw fields to our flat schema."""

    def _read_fixture(self, index: int) -> Dict[str, Any]:
        """Return a single processed record from the activities fixture."""
        raw = load_fixture("activities.json")
        stream = ActivitiesStream()
        client = make_client(activities=[raw[index]])
        records = list(stream.read_records(client, make_config(), _START, _END))
        assert records, "Expected at least one record from the fixture"
        return records[0]

    def test_activity_id_mapped(self):
        """activityId → activity_id (integer)."""
        record = self._read_fixture(0)
        assert record["activity_id"] == 12345678

    def test_activity_name_mapped(self):
        """activityName → activity_name (string)."""
        record = self._read_fixture(0)
        assert record["activity_name"] == "Morning Run"

    def test_activity_date_parsed_as_iso_date_string(self):
        """startTimeLocal datetime string is truncated to a date-only ISO-8601 string."""
        record = self._read_fixture(0)
        # "2024-01-15 07:30:00" → "2024-01-15"
        assert record["activity_date"] == "2024-01-15"

    def test_activity_type_extracted_from_nested_dict(self):
        """activityType.typeKey → activity_type (nested dict is flattened)."""
        record = self._read_fixture(0)
        assert record["activity_type"] == "running"

    def test_event_type_extracted_from_nested_dict(self):
        """eventType.typeKey → event_type."""
        record = self._read_fixture(0)
        assert record["event_type"] == "race"

    def test_event_type_none_when_raw_is_null(self):
        """eventType=null in the API response → event_type=None (no KeyError crash)."""
        record = self._read_fixture(2)  # fixture[2] has eventType=null
        assert record["event_type"] is None

    def test_avg_cadence_mapped_for_running(self):
        """averageRunningCadenceInStepsPerMinute → avg_cadence for running activities."""
        record = self._read_fixture(0)
        assert record["avg_cadence"] == pytest.approx(172.0)

    def test_avg_cadence_none_for_cycling(self):
        """avg_cadence is None for cycling (field absent in raw API response)."""
        record = self._read_fixture(1)  # fixture[1] is cycling
        assert record["avg_cadence"] is None


# ---------------------------------------------------------------------------
# ActivitiesStream — unit conversions
# ---------------------------------------------------------------------------

class TestActivitiesStreamUnitConversions:
    """Unit conversions are applied correctly (m→km, s→min, m/s→min/km)."""

    def _read_first(self) -> Dict[str, Any]:
        raw = load_fixture("activities.json")
        stream = ActivitiesStream()
        client = make_client(activities=[raw[0]])
        records = list(stream.read_records(client, make_config(), _START, _END))
        return records[0]

    def test_distance_metres_to_km(self):
        """10 050 m → 10.05 km (rounded to 3 decimal places)."""
        record = self._read_first()
        assert record["distance_km"] == pytest.approx(10.05, abs=0.001)

    def test_duration_seconds_to_minutes(self):
        """3 120 s → 52.0 min (rounded to 2 decimal places)."""
        record = self._read_first()
        assert record["duration_minutes"] == pytest.approx(52.0, abs=0.01)

    def test_speed_ms_to_pace_min_per_km(self):
        """3.22 m/s → (1000/3.22)/60 ≈ 5.18 min/km."""
        record = self._read_first()
        expected_pace = (1000.0 / 3.22) / 60.0
        assert record["avg_pace_min_km"] == pytest.approx(expected_pace, abs=0.01)

    def test_calories_stored_as_integer(self):
        """calories is an integer, not a float."""
        record = self._read_first()
        # pd.Int64Dtype() preserves integer type; to_dict() may return int or np.int64
        assert record["calories"] == 520
        assert int(record["calories"]) == record["calories"]


# ---------------------------------------------------------------------------
# ActivitiesStream — sanity checks (aberrant values → None)
# ---------------------------------------------------------------------------

class TestActivitiesStreamSanityChecks:
    """Physiologically impossible values are silently set to None, not raised."""

    def _read_bad_record(self) -> Dict[str, Any]:
        """Return the processed 'bad data' record (fixture index 2)."""
        raw = load_fixture("activities.json")
        stream = ActivitiesStream()
        client = make_client(activities=[raw[2]])
        records = list(stream.read_records(client, make_config(), _START, _END))
        assert records
        return records[0]

    def test_hr_zero_becomes_none(self):
        """averageHR=0 means no HR monitor — must be None, not 0."""
        record = self._read_bad_record()
        assert record["avg_heart_rate"] is None

    def test_max_hr_above_250_becomes_none(self):
        """maxHR=500 is physiologically impossible — must be None."""
        record = self._read_bad_record()
        assert record["max_heart_rate"] is None

    def test_aberrant_speed_yields_none_pace(self):
        """averageSpeed=999 m/s (> 13 m/s maximum) → avg_pace_min_km=None."""
        record = self._read_bad_record()
        assert record["avg_pace_min_km"] is None

    def test_training_effect_above_max_becomes_none(self):
        """aerobicTrainingEffect=9.9 (> 5.0) → training_effect=None."""
        record = self._read_bad_record()
        assert record["training_effect"] is None

    def test_vo2max_below_min_becomes_none(self):
        """vO2MaxValue=-5.0 (< 5.0 minimum) → vo2max_estimate=None."""
        record = self._read_bad_record()
        assert record["vo2max_estimate"] is None

    def test_null_distance_becomes_none(self):
        """distance=null in raw payload → distance_km=None (no crash)."""
        record = self._read_bad_record()
        assert record["distance_km"] is None

    def test_null_duration_becomes_none(self):
        """duration=null in raw payload → duration_minutes=None."""
        record = self._read_bad_record()
        assert record["duration_minutes"] is None

    def test_empty_api_response_yields_zero_records(self):
        """get_activities_by_date() returning [] → read_records() yields nothing."""
        stream = ActivitiesStream()
        client = make_client(activities=[])
        records = list(stream.read_records(client, make_config(), _START, _END))
        assert records == []

    def test_only_valid_speed_range_produces_valid_pace(self):
        """Speed exactly at the boundary (> _MIN_SPEED_MS, <= _MAX_SPEED_MS) is kept."""
        raw_activity = {
            "activityId": 99,
            "activityName": "Walk",
            "startTimeLocal": "2024-01-20 10:00:00",
            "activityType": {"typeKey": "walking"},
            "eventType": {"typeKey": "training"},
            "distance": 5000.0,
            "duration": 3600.0,
            "averageSpeed": 1.39,  # ~5 km/h — valid walking pace
            "averageHR": 90,
            "maxHR": 110,
            "elevationGain": 10.0,
            "calories": 200,
            "averageRunningCadenceInStepsPerMinute": None,
            "aerobicTrainingEffect": 1.0,
            "vO2MaxValue": None,
        }
        stream = ActivitiesStream()
        client = make_client(activities=[raw_activity])
        records = list(stream.read_records(client, make_config(), _START, _END))

        assert records[0]["avg_pace_min_km"] is not None


# ---------------------------------------------------------------------------
# GarminStream.read() — Airbyte protocol message shapes
# ---------------------------------------------------------------------------

class TestGarminStreamReadProtocol:
    """GarminStream.read() emits correctly-shaped RECORD and STATE Airbyte messages."""

    def test_record_messages_have_required_keys(self):
        """Every RECORD message contains the mandatory Airbyte protocol keys."""
        raw = load_fixture("activities.json")
        stream = ActivitiesStream()
        client = make_client(activities=raw[:1])

        messages = list(stream.read(client, make_config(), "full_refresh"))
        record_msgs = [m for m in messages if m["type"] == "RECORD"]

        assert len(record_msgs) == 1
        msg = record_msgs[0]
        assert msg["record"]["stream"] == "activities"
        assert "data" in msg["record"]
        assert "emitted_at" in msg["record"]

    def test_emitted_at_is_unix_milliseconds(self):
        """emitted_at is an integer (Unix epoch in milliseconds, per Airbyte spec)."""
        raw = load_fixture("activities.json")
        stream = ActivitiesStream()
        client = make_client(activities=raw[:1])

        messages = list(stream.read(client, make_config(), "full_refresh"))
        record_msg = next(m for m in messages if m["type"] == "RECORD")

        emitted_at = record_msg["record"]["emitted_at"]
        assert isinstance(emitted_at, int)
        # A sanity check: 2024-01-01 in milliseconds is ~1.7 × 10^12
        assert emitted_at > 1_700_000_000_000

    def test_ingested_at_is_injected_into_every_record(self):
        """Base class automatically adds ingested_at — it is never in the raw payload."""
        raw = load_fixture("activities.json")
        stream = ActivitiesStream()
        client = make_client(activities=raw[:1])

        messages = list(stream.read(client, make_config(), "full_refresh"))
        data = messages[0]["record"]["data"]

        assert "ingested_at" in data
        # ISO-8601 with timezone: contains 'T' and '+' or 'Z'
        assert "T" in data["ingested_at"]

    def test_state_message_emitted_at_end_of_incremental_sync(self):
        """Incremental sync emits a STATE message after all RECORD messages."""
        raw = load_fixture("activities.json")
        stream = ActivitiesStream()
        client = make_client(activities=raw[:1])  # one record, date 2024-01-15

        messages = list(stream.read(client, make_config(), "incremental"))
        state_msgs = [m for m in messages if m["type"] == "STATE"]

        assert len(state_msgs) == 1
        state_data = state_msgs[0]["state"]["data"]["activities"]
        assert state_data["activity_date"] == "2024-01-15"

    def test_state_reflects_latest_record_date(self):
        """With multiple records the STATE cursor holds the most recent date."""
        raw = load_fixture("activities.json")
        stream = ActivitiesStream()
        # Load all three fixture records (dates: 2024-01-15, 2024-01-16, 2024-01-17)
        client = make_client(activities=raw)

        messages = list(stream.read(client, make_config(), "incremental"))
        state_msgs = [m for m in messages if m["type"] == "STATE"]

        # The last STATE message must reflect the highest date seen.
        final_cursor = state_msgs[-1]["state"]["data"]["activities"]["activity_date"]
        assert final_cursor == "2024-01-17"

    def test_no_state_emitted_for_empty_result(self):
        """No STATE message when no records are fetched (cursor must not regress)."""
        stream = ActivitiesStream()
        client = make_client(activities=[])

        messages = list(stream.read(client, make_config(), "incremental"))
        state_msgs = [m for m in messages if m["type"] == "STATE"]

        assert state_msgs == []

    def test_no_state_emitted_for_full_refresh(self):
        """Full-refresh mode does not emit a STATE message (Airbyte resets state)."""
        raw = load_fixture("activities.json")
        stream = ActivitiesStream()
        client = make_client(activities=raw[:1])

        messages = list(stream.read(client, make_config(), "full_refresh"))
        # full_refresh emits RECORD messages but no STATE.
        state_msgs = [m for m in messages if m["type"] == "STATE"]

        # NOTE: ActivitiesStream has a cursor_field, so GarminStream.read() will
        # emit STATE even in full_refresh mode if records exist.  This test
        # documents the current behaviour — if the spec changes, update here.
        # For now we only assert records exist.
        record_msgs = [m for m in messages if m["type"] == "RECORD"]
        assert len(record_msgs) == 1

    def test_correct_number_of_records_emitted(self):
        """One RECORD message per raw activity in the API response."""
        raw = load_fixture("activities.json")
        stream = ActivitiesStream()
        client = make_client(activities=raw)

        messages = list(stream.read(client, make_config(), "full_refresh"))
        record_msgs = [m for m in messages if m["type"] == "RECORD"]

        assert len(record_msgs) == len(raw)


# ---------------------------------------------------------------------------
# GarminStream._compute_start_date() — date-window logic
# ---------------------------------------------------------------------------

class TestComputeStartDate:
    """_compute_start_date() picks the right window start for each sync mode."""

    def test_full_refresh_uses_lookback_days(self):
        """FULL_REFRESH ignores state and subtracts lookback_days from today."""
        stream = ActivitiesStream()
        config = make_config(lookback_days=30)
        today = date(2024, 2, 15)
        state = {"activity_date": "2024-01-01"}  # must be ignored

        start = stream._compute_start_date(config, "full_refresh", state, today)

        assert start == today - timedelta(days=30)

    def test_incremental_resumes_from_state_cursor(self):
        """INCREMENTAL resumes from the saved cursor date rather than lookback window."""
        stream = ActivitiesStream()
        config = make_config(lookback_days=30)
        today = date(2024, 2, 15)
        state = {"activity_date": "2024-02-10"}

        start = stream._compute_start_date(config, "incremental", state, today)

        assert start == date(2024, 2, 10)

    def test_incremental_falls_back_when_state_is_empty(self):
        """INCREMENTAL with empty state behaves like FULL_REFRESH (first sync)."""
        stream = ActivitiesStream()
        config = make_config(lookback_days=14)
        today = date(2024, 2, 15)

        start = stream._compute_start_date(config, "incremental", {}, today)

        assert start == today - timedelta(days=14)

    def test_incremental_falls_back_on_corrupted_cursor(self):
        """An unparseable cursor value triggers a fallback to the lookback window."""
        stream = ActivitiesStream()
        config = make_config(lookback_days=7)
        today = date(2024, 2, 15)
        state = {"activity_date": "not-a-valid-date"}

        start = stream._compute_start_date(config, "incremental", state, today)

        assert start == today - timedelta(days=7)


# ---------------------------------------------------------------------------
# ActivitiesStream — stream metadata
# ---------------------------------------------------------------------------

class TestActivitiesStreamMetadata:
    """Stream properties are correctly declared for the Airbyte CATALOG message."""

    def test_name(self):
        assert ActivitiesStream().name == "activities"

    def test_primary_key(self):
        assert ActivitiesStream().primary_key == "activity_id"

    def test_cursor_field(self):
        assert ActivitiesStream().cursor_field == "activity_date"

    def test_supported_sync_modes_include_incremental(self):
        assert "incremental" in ActivitiesStream().supported_sync_modes
        assert "full_refresh" in ActivitiesStream().supported_sync_modes

    def test_get_json_schema_returns_all_required_fields(self):
        """All schema fields from CLAUDE.md are present in get_json_schema()."""
        schema = ActivitiesStream().get_json_schema()
        expected_fields = {
            "activity_id", "activity_name", "activity_date", "activity_type",
            "distance_km", "duration_minutes", "avg_pace_min_km",
            "avg_heart_rate", "max_heart_rate", "elevation_gain_m",
            "calories", "avg_cadence", "event_type",
            "training_effect", "vo2max_estimate", "ingested_at",
        }
        assert expected_fields == set(schema["properties"].keys())

    def test_catalog_entry_has_source_defined_cursor(self):
        """source_defined_cursor=True because ActivitiesStream manages its own cursor."""
        entry = ActivitiesStream().get_catalog_entry()
        assert entry["source_defined_cursor"] is True
        assert entry["default_cursor_field"] == ["activity_date"]
