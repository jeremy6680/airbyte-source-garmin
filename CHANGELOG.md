# CHANGELOG

All notable changes to `airbyte-source-garmin` are documented here.

Format: `[version] YYYY-MM-DD — description`

---

## [0.1.3] — 2026-04-25

### Fixed

- **KB-013 / ADR-024** — `CalendarEventsStream` emitted 0 records on every sync.
  `get_calendar_week(year, week)` does not exist in `garminconnect` 0.3.x; every
  call raised `AttributeError` silently caught by the per-week exception handler.
  Replaced with `get_scheduled_workouts(year, month)` (month-by-month iteration),
  filtered to `itemType == "event"` items only, deduplicated by URL, and generated
  a synthetic `event_id` via `hash(title|date)` since the API returns `null` for
  event IDs.

### Changed

- `DECISIONS.md` — ADR-020 marked superseded; ADR-024 added.
- `KNOWN_BUGS.md` — KB-006 updated (per-week → per-month); KB-010 marked superseded;
  KB-013 added.
- `README.md` — Docker image tag corrected to `source-garmin:dev`; added "Deploying
  to a local Airbyte instance (abctl)" section with `docker save` / `ctr import`
  procedure.

---

## [0.1.2] — 2026-04-25

### Fixed

- **KB-012 / ADR-023** — `garminconnect` 0.3.x renamed the internal OAuth library
  attribute from `garth` to `client`, breaking session persistence (`AttributeError`
  on every `garth.dump()`/`garth.load()` call). Updated `auth.py` to use
  `client.client.load()`/`client.client.dump()`.
- **KB-012** — Session validation used `client.get_full_name()` which in 0.3.x
  returns the cached `self.full_name` (always `None` on a fresh instance) without
  making a network call. Replaced with
  `client.connectapi("/userprofile-service/socialProfile")` which validates the
  token with a real HTTP request *and* populates `client.display_name` so that
  `get_user_summary()` no longer raises `GarminConnectConnectionError`. This was
  causing `DailyHealthStream` to silently return 0 records after every session
  restore.

---

## [0.1.1] — 2026-04-25

### Fixed

- **KB-006** — HTTP 429 during API reads was not retried.  Extracted retry logic
  into `source_garmin/utils.py: retry_on_429()` (single source of truth for the
  30 s / 60 s / 120 s backoff schedule).  All three stream `read_records()` methods
  now wrap their Garmin API calls with it; `GarminAuth._login_with_retry()` was
  simplified to delegate to the same utility.  Added `unit_tests/test_utils.py`
  with 8 tests covering the full retry surface (ADR-022).
- **KB-002** — deleted empty `source_garmin/manifest.yaml` leftover.
- **KB-003** — deleted empty `metadata.yaml` leftover (not targeting the Airbyte
  connector registry).

---

## [0.1.0] — 2026-04-25

Initial release. Fully functional custom Airbyte source connector for Garmin Connect.

### Added

**Streams**
- `activities` — one record per Garmin Connect activity; FULL_REFRESH + INCREMENTAL (cursor: `activity_date`); unit conversions (m→km, s→min, m/s→min/km); physiological sanity checks on HR, pace, VO2max, training effect
- `daily_health` — one record per calendar day; FULL_REFRESH + INCREMENTAL (cursor: `date`); flattens the `lastNight` nested object; resting HR sanity check
- `calendar_events` — upcoming races and training events; FULL_REFRESH only; forward-looking window (past `lookback_days` + 365 days ahead); deduplication across ISO week boundaries

**Infrastructure**
- `ConnectorConfig` — Pydantic v2 settings with validation; `lookback_days` clamped to 1–365; `SecretStr` for password; JSON Schema SPEC builder
- `GarminAuth` — SSO login via `garminconnect`; session persistence via `garth.dump()`/`garth.load()`; exponential backoff on HTTP 429 (30s, 60s, 120s, 3 attempts)
- `GarminStream` — abstract base class handling RECORD/STATE message formatting, `ingested_at` injection, incremental cursor tracking, date-window calculation
- `SourceGarmin` — `check`, `discover`, `read` orchestrator; single login per sync run; state namespaced per stream
- `main.py` — Airbyte CLI entrypoint (`spec`, `check`, `discover`, `read`)
- `Dockerfile` — `python:3.11-slim`; dependency layer cached separately from source code

**Tests**
- 99 unit tests across `test_auth.py` (14) and `test_streams.py` (85)
- Fixtures in `unit_tests/fixtures/` mirror real Garmin API response shapes
- Mock boundary: only `garminconnect.Garmin` is mocked; all transformation logic runs for real

**Documentation**
- `DECISIONS.md` — 20 ADRs covering every non-obvious technical choice
- `KNOWN_BUGS.md` — 11 tracked issues with fix plans and resolution notes
- `README.md` — setup, usage, Docker instructions, project structure
