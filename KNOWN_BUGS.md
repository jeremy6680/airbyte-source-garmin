# KNOWN_BUGS.md — Open Issues & Mismatches

This file tracks known issues, naming mismatches, and deferred fixes so that
nothing falls through the cracks across development steps.

---

## KB-001 — `calendar.py` should be `calendar_events.py`

**Severity**: Low (build-time, not runtime)  
**Introduced**: Initial scaffold  
**Fixed**: Step 12 — `git mv` + import updates in `source.py` and `test_streams.py`

### Description
The scaffolded file is `source_garmin/streams/calendar.py`, but CLAUDE.md specifies
the canonical name `source_garmin/streams/calendar_events.py`.

The name `calendar` also shadows Python's built-in `calendar` standard-library
module, which could cause confusing `ImportError` messages if any dependency
imports it.

The stream (`CalendarEventsStream`) was fully implemented in Step 11 inside
`calendar.py` rather than being moved to `calendar_events.py` as originally planned.
The fix now requires a rename rather than a rewrite.

### Fix plan
```bash
git mv source_garmin/streams/calendar.py source_garmin/streams/calendar_events.py
```
Update the import in `source_garmin/source.py`:
```python
from source_garmin.streams.calendar_events import CalendarEventsStream
```
Update the import in `unit_tests/test_streams.py` accordingly.

### Affected files
- `source_garmin/streams/calendar.py` — to be renamed
- `source_garmin/source.py` — import path to be updated
- `unit_tests/test_streams.py` — import path to be updated

---

## KB-002 — `source_garmin/manifest.yaml` is an empty leftover

**Severity**: Cosmetic  
**Introduced**: Initial scaffold  
**Fixed**: v0.1.1 — file deleted

### Description
`source_garmin/manifest.yaml` was created by the initial scaffold but is empty.
It is only meaningful for Airbyte's declarative connector builder and has no role
in this low-level Python connector.

---

## KB-003 — `metadata.yaml` is an empty leftover

**Severity**: Cosmetic  
**Introduced**: Initial scaffold  
**Fixed**: v0.1.1 — file deleted (not targeting the Airbyte connector registry)

### Description
`metadata.yaml` at the project root is empty. Airbyte uses this file in its
connector registry to declare connector metadata (name, icon, version, etc.).
It is not required for local or Docker-based operation, and the project does not
target the Airbyte registry, so the file was removed.

---

## KB-004 — `password` field is `str`, not `SecretStr` (real security issue)

**Severity**: Medium (credential leak in logs/repr)  
**Introduced**: Step 2 (Config)  
**Fixed**: Step 3 (Auth) — resolved in `config.py` rewrite

### Description
The module docstring (line 8) and the class docstring (lines 24–25) both claim the
password is stored as `SecretStr` so it never appears in log output. The actual
field declaration is:

```python
password: str = Field(...)
```

Because it is plain `str`, calling `repr(config)` or letting loguru log the config
object will print the password in cleartext.

### Reproduction
```python
cfg = ConnectorConfig(email="a@b.com", password="s3cr3t")
print(repr(cfg))   # password='s3cr3t' — plaintext
```

### Fix plan
1. Change the field type to `SecretStr` (from `pydantic`).
2. Update `password_must_not_be_empty` validator to accept and return `SecretStr`.
3. Update `load_config()` callers (Step 3, auth.py) to call
   `config.password.get_secret_value()` when passing the password to
   `garminconnect.Garmin()`.
4. Fix the module docstring to remove the false claim on line 8.

### Affected files
- `source_garmin/config.py` — field type + validator + docstring
- `source_garmin/auth.py` — caller must use `.get_secret_value()` (Step 3)

---

## KB-005 — Class docstring leaks into the generated SPEC `description` field

**Severity**: Low (cosmetic, but visible in the Airbyte UI)  
**Introduced**: Step 2 (Config)  
**Fixed**: Step 3 (Auth) — `build_spec()` now pops `description` from schema

### Description
`ConnectorConfig.model_json_schema()` includes the full class docstring in the
`description` property of the generated JSON Schema. The smoke-test output confirms
this — the SPEC `connectionSpecification.description` contains the multi-line
Google-style docstring with `Attributes:` section, which would appear verbatim in
the Airbyte UI as the connection description.

### Fix plan
Override `model_json_schema()` in `build_spec()` to `pop("description", None)` from
the schema before returning it, keeping field-level descriptions (which are useful)
while dropping the class-level one (which is implementation noise).

---

## KB-006 — HTTP 429 during API reads is not retried

**Severity**: Medium (could cause sync failures on large accounts)  
**Introduced**: Step 5 (Activities stream)  
**Fixed**: v0.1.1 — extracted retry logic to `source_garmin/utils.py: retry_on_429()`; all three stream `read_records()` methods and `GarminAuth._login_with_retry()` now delegate to it

### Description
`GarminAuth._login_with_retry()` retried on HTTP 429 for the login step only.
API read calls (`client.get_activities_by_date()`, etc.) could also return 429 if
too many requests are made in a short window — this is especially likely on first
sync of a large account with years of history. Previously the exception propagated
uncaught (activities) or was silently skipped (daily_health, calendar_events).

### Fix
Created `source_garmin/utils.py` with `retry_on_429(fn, delays)`.  The function
is the single source of truth for the backoff schedule (30 s, 60 s, 120 s).
`GarminAuth._login_with_retry()` was simplified to delegate to it, and all three
stream `read_records()` methods wrap their API calls with it.  `test_utils.py`
covers the full retry behaviour independently.

### Affected files
- `source_garmin/utils.py` — new file, defines `retry_on_429`
- `source_garmin/streams/activities.py` — `read_records()` API call
- `source_garmin/streams/daily_health.py` — per-day API call
- `source_garmin/streams/calendar_events.py` — per-month API call
- `source_garmin/auth.py` — `_login_with_retry()` now delegates to `retry_on_429`

---

## KB-007 — `avg_cadence` is populated only for running activities

**Severity**: Low (expected null for non-running sports)  
**Introduced**: Step 5 (Activities stream)  
**Target fix**: Post-Step 11 enhancement, if needed

### Description
The raw Garmin field `averageRunningCadenceInStepsPerMinute` is sport-specific —
it is only populated for running activities. For cycling, swimming, or strength
training, this field is absent from the API response and `avg_cadence` will always
be `None`.

Garmin exposes cycling cadence under a different field name
(`averageBikingCadenceInRevPerMinute`), which is not currently mapped.

### Fix plan
If multi-sport cadence support is needed, add a helper in `_normalize_raw()` that
checks the `activityType.typeKey` and reads the appropriate cadence field. For now,
`None` is the correct and documented behaviour for non-running activities.

---

## KB-008 — `GarminStream.read()` emits STATE in `full_refresh` mode

**Severity**: Low (protocol inconsistency, no data loss)  
**Introduced**: Step 4 (Base stream)  
**Fixed**: Step 12 — added `sync_mode == "incremental"` guard in `GarminStream.read()`; test tightened to assert `state_msgs == []`

### Description
`GarminStream.read()` emits a `STATE` message whenever records are fetched and
`cursor_field` is set — regardless of the `sync_mode` argument:

```python
# base.py — the condition does not check sync_mode
if self.cursor_field and latest_cursor:
    yield self._make_state_message({self.cursor_field: latest_cursor})
```

For a `full_refresh` run this is technically unnecessary: Airbyte's full-refresh
mode replaces the destination table entirely and does not use saved state. Emitting
STATE here does not cause incorrect data, but it pollutes the output stream with a
message that carries no useful information in this sync mode.

This is documented in `unit_tests/test_streams.py` in the
`test_no_state_emitted_for_full_refresh` test, which currently accepts the current
behaviour rather than asserting that STATE must be absent.

### Fix plan
Guard the final STATE emit with a `sync_mode == "incremental"` check in
`GarminStream.read()`. Update the test to assert that `full_refresh` produces no
STATE messages.

### Affected files
- `source_garmin/streams/base.py` — add sync_mode guard to the STATE emit
- `unit_tests/test_streams.py` — tighten the `test_no_state_emitted_for_full_refresh` assertion

---

## KB-009 — Virtual environment uses Python 3.14, not Python 3.11

**Severity**: Low (no current failures, but a latent risk)  
**Introduced**: Step 1 (Setup) — venv created before constraint was enforced  
**Target fix**: Whenever a clean environment is set up (Docker build covers this)

### Description
CLAUDE.md mandates Python 3.11. The `.venv` in the project root was created with
the Homebrew default (`python3`, currently 3.14). All 53 unit tests pass, but:

- Any package that ships a compiled `.so` extension (e.g. numpy, pandas internals)
  will use the 3.14 ABI, not the 3.11 ABI. Behaviour differences between minor
  versions could produce false-positive test results for version-specific edge
  cases.
- The Docker image (`FROM python:3.11-slim`) uses 3.11. If a 3.14 behaviour
  difference ever causes a connector to behave differently locally vs. in Docker,
  this mismatch is the first thing to investigate.

### Fix plan
Recreate the venv with an explicit Python 3.11 binary:
```bash
python3.11 -m venv .venv   # requires pyenv or homebrew python@3.11
pip install -r requirements-dev.txt
```
The Docker build is unaffected (it pins `python:3.11-slim`), so this is a local
developer environment concern only.

---

## KB-010 — `_CAL_END` test variable has no effect on CalendarEventsStream

**Severity**: Low (tests pass, but documentation is misleading)  
**Introduced**: Step 11 (CalendarEventsStream + tests)  
**Superseded**: v0.1.3 — `read_records()` was completely rewritten (ADR-024). The
`get_calendar_week` mock no longer exists in the code; tests mock
`get_scheduled_workouts` instead (see KB-013). The `_CAL_END` concern is still
relevant for the rewritten tests — deduplication now uses URL as the key and the
API response shape changed to `{"calendarItems": [...]}`.

### Description (historical)
`CalendarEventsStream.read_records()` ignores the `end_date` argument from the base
class and replaces it internally with `date.today() + 365 days`. The `_CAL_END`
test variable had no effect; tests passed only because `return_value` caused every
mock call to return the same fixture and the deduplication set discarded repeats.

### Affected files
- `unit_tests/test_streams.py` — CalendarEventsStream tests need updating (KB-013)

---

## KB-011 — DailyHealthStream state test relies on silent exception swallowing

**Severity**: Low (test passes, but is fragile and impure)  
**Introduced**: Step 11 (DailyHealthStream tests)  
**Fixed**: Step 12 — test rewritten to call `read_records()` directly on `_DH_START → _DH_END` (2 days, 2 mock calls) and build the STATE assertion manually

### Description
`test_state_message_emitted_at_end_of_incremental_sync` calls `stream.read()` (the
base-class orchestrator) with a default config of `lookback_days=30`. This triggers
~31 sequential `get_user_summary()` calls (one per day in the 30-day window). But
the mock is configured with only 2 items in `side_effect`:

```python
client = make_health_client(daily_records=raw)  # raw has 2 items
messages = list(stream.read(client, make_config(), "incremental", {}))
```

After the 2 items are consumed, MagicMock raises `StopIteration` on every
subsequent call. Inside the `DailyHealthStream.read_records()` generator,
`StopIteration` is a subclass of `Exception` and is caught by the per-day
`except Exception` handler, which logs a warning and skips the day silently.

The test passes because the 2 valid records are processed before the mock is
exhausted, and the STATE cursor ("2024-01-16") is set correctly. However:
- The test relies on exception-handling behaviour that was designed for genuine
  network failures, not for test scaffolding.
- 28 spurious `WARNING` log lines are emitted during the test run.
- A future refactor of the exception handler (e.g. only catching specific Garmin
  exceptions) would break the test unexpectedly.

### Fix plan
Replace `stream.read()` with a direct `stream.read_records()` call over the tight
`_DH_START → _DH_END` window (2 days → exactly 2 mock calls), then reconstruct the
STATE assertion manually. This removes the dependence on accidental exception
handling.

```python
records = list(stream.read_records(client, make_config(), _DH_START, _DH_END))
# assert STATE manually from the cursor tracking logic
```

### Affected files
- `unit_tests/test_streams.py` — `TestDailyHealthStreamMetadata.test_state_message_emitted_at_end_of_incremental_sync`

---

## KB-012 — `garminconnect` 0.3.x renamed `garth` → `client` and broke session persistence for `DailyHealthStream`

**Severity**: High (DailyHealthStream silently returned 0 records on every sync after session restore)  
**Introduced**: v0.1.0 (written against the old garth-based API)  
**Fixed**: v0.1.2 — two changes to `source_garmin/auth.py`:
  1. `client.garth.load/dump` → `client.client.load/dump`
  2. Replaced `client.get_full_name()` with `client.connectapi("/userprofile-service/socialProfile")` for session validation

### Description
`garminconnect` 0.3.x replaced the `garth` OAuth library with an internal `Client`
object. The old `client.garth` attribute no longer exists, so every call that
touched it raised `AttributeError: 'Garmin' object has no attribute 'garth'`.

Separately, `_try_load_session` used `client.get_full_name()` to validate the token.
In 0.3.x, `get_full_name()` just returns the cached `self.full_name` (None for a
fresh instance) — it makes no network call and never raises. The token appeared
valid even for a brand-new unauthenticated instance. More critically, `display_name`
remained `None`, causing `get_user_summary()` to raise
`GarminConnectConnectionError("Display name is not set")` on every daily health call.
The per-day `except Exception` handler in `DailyHealthStream.read_records()` silently
skipped every day, producing 0 records with no fatal error.

---

## KB-013 — Unit tests for CalendarEventsStream mock the wrong method

**Severity**: Medium (unit tests pass but test nothing meaningful)  
**Introduced**: v0.1.3 — `read_records()` rewritten to use `get_scheduled_workouts`  
**Target fix**: Next test update pass

### Description
`unit_tests/test_streams.py` mocks `client.get_calendar_week` on the stream's
client. Since `get_calendar_week` no longer exists in `garminconnect` 0.3.x
*and* is no longer called by the rewritten `read_records()`, the mock intercepts
nothing. The test will either:
- Pass vacuously (the mock method is never called, the loop calls the real
  `get_scheduled_workouts` which, on a `MagicMock`, returns another `MagicMock`
  instead of a dict with `calendarItems`), or
- Fail with an unexpected return type when the code does
  `response.get("calendarItems", [])` on a `MagicMock`.

### Fix plan
1. Change the mock target from `get_calendar_week` to `get_scheduled_workouts`.
2. Update the fixture/return value to wrap items in `{"calendarItems": [...]}`.
3. Rename `_CAL_END` annotation (see KB-010) and update call counts accordingly.
4. Update the `event_id` assertion — field is now a synthetic hash-based int,
   not the raw `id` from the fixture.
5. Deduplication key changes from event `id` to event `url`.

### Affected files
- `unit_tests/test_streams.py` — all `TestCalendarEventsStream` tests
- `unit_tests/fixtures/calendar_events.json` — response must now be
  `{"calendarItems": [...]}` with `itemType: "event"` items

---

### Why unit tests did not catch this
Unit tests mock `garminconnect.Garmin` entirely. `MagicMock()` auto-creates
`mock_client.garth.load` on attribute access — so the tests always passed.
Only integration tests against the real library exposed the breakage.

### Fix (ADR-023)
- `client.client.load/dump(path)` replaces `client.garth.load/dump(path)`
- `client.connectapi("/userprofile-service/socialProfile")` replaces
  `client.get_full_name()` — it validates the token with a real network call AND
  populates `client.display_name`/`client.full_name` so all downstream endpoints work
