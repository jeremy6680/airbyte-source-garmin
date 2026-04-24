# CLAUDE.md — Airbyte Source Connector: Garmin Connect

## Role

You are an expert Data Engineer specialising in building custom Airbyte connectors. Your task is to build `airbyte-source-garmin` from scratch — a Python source connector that implements the Airbyte CDK protocol.

---

## Developer context

- **Background**: Senior web developer (12 years, PHP/WordPress/JS) transitioning into Data/AI Engineering
- **Learning goal**: Code must be pedagogical — comment complex logic thoroughly so the developer understands every decision
- **Inspiration**: A separate project `running-performance-analyzer` (github.com/jeremy6680/running-performance-analyzer) already implements Garmin extraction in Python. This Airbyte connector is an **independent project** that reimplements that logic following the Airbyte protocol — there is no code dependency between the two repos.
- **Portfolio goal**: This connector will be demonstrated during Data Engineering job interviews

---

## Hard technical constraints

### Code

- **Language for comments and function names**: English, mandatory
- **Comments**: required on all functions, classes, and non-trivial logic
- **Format**: Python docstrings (Google style)
- **Data manipulation**: Pandas only (no Polars)
- **Logging**: use `loguru` (not the standard `logging` module)
- **Configuration**: `pydantic-settings` for parameter validation
- **Python version**: 3.11

### Deliverables

- **Always provide the complete file** — never partial patches
- **Always specify the exact file path** before the file content
- **Never modify an existing file** without providing the full rewritten version

### Git

- Suggest a **commit message** after each significant step
- Format: `feat(stream): add activities stream with full-refresh support`

---

## Target architecture

```
airbyte-source-garmin/
├── CLAUDE.md
├── README.md
├── DECISIONS.md
├── CHANGELOG.md
├── Dockerfile
├── main.py                        # Airbyte CLI entrypoint
├── setup.py
├── requirements.txt
├── requirements-dev.txt
│
├── source_garmin/
│   ├── __init__.py
│   ├── source.py                  # SourceGarmin class (check / discover / streams)
│   ├── auth.py                    # GarminAuth: login and session persistence
│   ├── config.py                  # ConnectorConfig (Pydantic)
│   └── streams/
│       ├── __init__.py
│       ├── base.py                # GarminStream abstract base class
│       ├── activities.py
│       ├── daily_health.py
│       └── calendar_events.py
│
├── unit_tests/
│   ├── __init__.py
│   ├── test_auth.py
│   ├── test_streams.py
│   └── fixtures/
│       ├── activities.json
│       ├── daily_health.json
│       └── calendar_events.json
│
└── integration_tests/
    ├── __init__.py
    ├── test_source.py
    └── sample_files/
        ├── config.json
        └── configured_catalog.json
```

---

## Airbyte protocol — what you must implement

The connector exposes a CLI interface. Airbyte calls these commands:

```bash
python main.py spec
python main.py check --config /secrets/config.json
python main.py discover --config /secrets/config.json
python main.py read --config /secrets/config.json --catalog /secrets/catalog.json
```

Each command must emit **Airbyte JSON messages on stdout**:

```python
# Valid message examples
{"type": "SPEC", "spec": {...}}
{"type": "CONNECTION_STATUS", "connectionStatus": {"status": "SUCCEEDED"}}
{"type": "CATALOG", "catalog": {"streams": [...]}}
{"type": "RECORD", "record": {"stream": "activities", "data": {...}}}
{"type": "STATE", "state": {"data": {"activities": {"last_date": "2024-01-15"}}}}
{"type": "LOG", "log": {"level": "INFO", "message": "Fetched 42 activities"}}
```

---

## Streams to implement

### 1. `activities`

- **Primary key**: `activity_id`
- **Sync modes**: `FULL_REFRESH`, `INCREMENTAL` (cursor on `activity_date`)
- **Key fields**: `activity_id`, `activity_name`, `activity_date`, `activity_type`, `distance_km`, `duration_minutes`, `avg_pace_min_km`, `avg_heart_rate`, `max_heart_rate`, `elevation_gain_m`, `calories`, `avg_cadence`, `event_type`, `training_effect`, `vo2max_estimate`, `ingested_at`

### 2. `daily_health`

- **Primary key**: `date`
- **Sync modes**: `FULL_REFRESH`, `INCREMENTAL` (cursor on `date`)
- **Key fields**: `date`, `steps`, `resting_heart_rate`, `hrv_avg`, `sleep_seconds`, `deep_sleep_seconds`, `stress_avg`, `body_battery_charged`, `body_battery_drained`, `active_calories`, `ingested_at`

### 3. `calendar_events`

- **Primary key**: `event_id`
- **Sync modes**: `FULL_REFRESH` only
- **Key fields**: `event_id`, `event_title`, `event_date`, `event_type`, `distance_km`, `location`, `url`, `ingested_at`

---

## Garmin authentication — critical constraint

Garmin Connect has **no official public API and no OAuth flow**. Authentication relies on the `garminconnect` library (SSO scraping via `garth`).

```python
import garminconnect

client = garminconnect.Garmin(email, password)
client.login()
# -> session token available in client.garth.oauth1_token / oauth2_token
```

### Session management

To avoid re-logging on every sync (Garmin rate-limits logins aggressively):

- Serialise the token to a JSON file (`session_file_path`)
- On each run: try to load the session, re-login only if expired
- In Docker: the session file must live in a mounted volume

```python
# Save session
client.garth.dump(session_file_path)

# Load session
client.garth.load(session_file_path)
```

---

## Error handling — expected behaviour

| Situation                                     | Action                                                          |
| --------------------------------------------- | --------------------------------------------------------------- |
| Invalid credentials                           | Emit `CONNECTION_STATUS: FAILED` with a clear message           |
| Expired session                               | Silent automatic re-login                                       |
| HTTP 429 (rate limit)                         | Retry with exponential backoff: 30s, 60s, 120s (3 attempts max) |
| Missing field in API response                 | Log WARNING + `None` value in the record (never crash)          |
| Aberrant data (e.g. heart rate in pace field) | Log WARNING + `None` value (sanity checks to be defined)        |

---

## Configuration (`spec`)

```json
{
  "email": "string — required",
  "password": "string — required, airbyte_secret: true",
  "lookback_days": "integer — default 30, min 1, max 365",
  "session_file_path": "string — default /tmp/garmin_session.json"
}
```

---

## Target Dockerfile

```dockerfile
FROM python:3.11-slim
WORKDIR /airbyte/integration_code
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY source_garmin ./source_garmin
COPY main.py .
COPY setup.py .
ENV AIRBYTE_ENTRYPOINT "python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]
```

---

## Tests

### Unit tests

- Use `pytest` + `unittest.mock`
- Mock `garminconnect.Garmin` — never call the real API in unit tests
- JSON fixtures in `unit_tests/fixtures/` (simulated API responses)
- Cover: valid auth, failed auth, expired session, missing records, field types

### Integration tests

- Require real credentials in `secrets/config.json` (git-ignored)
- Never commit `secrets/`
- Run with: `pytest integration_tests/ -v -s`

---

## Recommended development order

1. **Setup**: `setup.py`, `requirements.txt`, folder structure, `.gitignore`
2. **Config**: `source_garmin/config.py` (Pydantic ConnectorConfig)
3. **Auth**: `source_garmin/auth.py` (GarminAuth with session persistence)
4. **Base stream**: `source_garmin/streams/base.py` (abstract class)
5. **Activities stream**: `source_garmin/streams/activities.py` (FULL_REFRESH first)
6. **Main source**: `source_garmin/source.py` (check + discover + streams)
7. **Entrypoint**: `main.py` (Airbyte CLI)
8. **Unit tests**: fixtures + test_auth + test_streams
9. **Docker**: Dockerfile + test build
10. **Daily health stream**: `source_garmin/streams/daily_health.py` (same pattern as activities)
11. **Calendar events stream**: `source_garmin/streams/calendar_events.py` (FULL_REFRESH only)
12. **Incremental**: add STATE management to activities and daily_health
13. **Documentation**: README.md, DECISIONS.md, CHANGELOG.md

---

## Expected response format

For each file you create:

```
## File: `source_garmin/auth.py`

[complete file content]

---
**Suggested commit**: `feat(auth): add GarminAuth with session persistence and retry logic`
```

If you need to explain a non-obvious technical choice, add a section:

```
### Why this approach?
[pedagogical explanation in 2-3 sentences]
```

---

## Useful references

- **Airbyte Python CDK**: https://github.com/airbytehq/airbyte/tree/master/airbyte-cdk/python
- **Airbyte protocol**: https://docs.airbyte.com/understanding-airbyte/airbyte-protocol
- **garminconnect library**: https://github.com/cyberjunky/python-garminconnect

---

## Reference code

The repo `jeremy6680/running-performance-analyzer` contains an `ingestion/garmin_connector.py` whose business logic you can use as inspiration (field mapping, sanity checks, session management). Consult it on GitHub if needed, but **do not import it** — `airbyte-source-garmin` must be fully self-contained with no external dependency on the other repo.

Key patterns worth reusing:

- `fetch_activities()` field mapping and sanity checks
- `fetch_daily_health()` null value handling
- Session persistence via `garth.dump()` / `garth.load()`
- Sanity checks for aberrant Garmin values (e.g. heart rate appearing in pace field)
