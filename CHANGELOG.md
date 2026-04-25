# CHANGELOG

All notable changes to `airbyte-source-garmin` are documented here.

Format: `[version] YYYY-MM-DD — description`

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
