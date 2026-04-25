# airbyte-source-garmin

A custom [Airbyte](https://airbyte.com) source connector that syncs data from [Garmin Connect](https://connect.garmin.com) — activities, daily health metrics, and calendar events — to any Airbyte-supported destination.

Built from scratch in Python without the Airbyte CDK, following the Airbyte protocol directly.

---

## Streams

| Stream | Primary key | Sync modes | Cursor |
|--------|-------------|------------|--------|
| `activities` | `activity_id` | full\_refresh, incremental | `activity_date` |
| `daily_health` | `date` | full\_refresh, incremental | `date` |
| `calendar_events` | `event_id` | full\_refresh | — |

### `activities`
One record per Garmin Connect activity (runs, rides, swims, etc.). Includes distance, duration, pace, heart rate, elevation, cadence, VO2max estimate, and training effect. Unit conversions applied: metres → km, seconds → minutes, m/s → min/km.

### `daily_health`
One record per calendar day. Aggregates steps, resting heart rate, sleep duration, stress score, body battery, and HRV weekly average.

### `calendar_events`
Upcoming races and training events from the Garmin calendar. Always queries the past `lookback_days` plus 365 days forward to capture future race registrations.

---

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `email` | string | yes | — | Garmin Connect account email |
| `password` | string (secret) | yes | — | Garmin Connect password |
| `lookback_days` | integer | no | 30 | How many days back to fetch on full\_refresh (1–365) |
| `session_file_path` | string | no | `/tmp/garmin_session.json` | Path for persisting the Garmin SSO session token |

> **Note:** Garmin Connect has no official public API. Authentication uses the `garminconnect` library which scrapes the SSO login flow. Garmin rate-limits login attempts aggressively — the connector retries automatically on HTTP 429 with exponential backoff (30s, 60s, 120s).

---

## Running locally

### Prerequisites

- Python 3.11+
- A Garmin Connect account

### Install

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Create a config file

```bash
mkdir -p secrets
cat > secrets/config.json << 'EOF'
{
  "email": "your@email.com",
  "password": "yourpassword",
  "lookback_days": 30,
  "session_file_path": "/tmp/garmin_session.json"
}
EOF
```

### Run Airbyte commands

```bash
# Print the connector spec
python main.py spec

# Validate credentials
python main.py check --config secrets/config.json

# Discover available streams
python main.py discover --config secrets/config.json

# Sync data (outputs Airbyte JSON messages on stdout)
python main.py read --config secrets/config.json --catalog secrets/catalog.json
```

### Example catalog file

```json
{
  "streams": [
    {
      "stream": { "name": "activities" },
      "sync_mode": "incremental",
      "destination_sync_mode": "append_dedup"
    },
    {
      "stream": { "name": "daily_health" },
      "sync_mode": "incremental",
      "destination_sync_mode": "append_dedup"
    },
    {
      "stream": { "name": "calendar_events" },
      "sync_mode": "full_refresh",
      "destination_sync_mode": "overwrite"
    }
  ]
}
```

---

## Running with Docker

The image is published on Docker Hub as [`jeremy6680/source-garmin`](https://hub.docker.com/r/jeremy6680/source-garmin).

```bash
# Pull the latest published image (no build needed)
docker pull jeremy6680/source-garmin:latest

# Or build locally from source
docker build -t source-garmin:dev .

# Run spec (no config needed)
docker run --rm jeremy6680/source-garmin:latest spec

# Run check
docker run --rm \
  -v $(pwd)/secrets:/secrets \
  source-garmin:dev \
  check --config /secrets/config.json

# Run read
docker run --rm \
  -v $(pwd)/secrets:/secrets \
  -v /tmp:/tmp \
  source-garmin:dev \
  read --config /secrets/config.json --catalog /secrets/catalog.json
```

> The `-v /tmp:/tmp` mount gives the container access to the session file at `/tmp/garmin_session.json`. For a persistent mount, replace `/tmp` with a named volume path.

---

## Deploying to a local Airbyte instance (abctl)

Airbyte installed via `abctl` runs on a KIND (Kubernetes IN Docker) cluster. The
cluster has its own containerd image registry — images built with
`docker build` are **not** automatically visible inside the cluster. You must load
them manually.

```bash
# 1. Build the image
docker build -t source-garmin:dev .

# 2. Export it to a tar archive
docker save source-garmin:dev -o /tmp/source-garmin.tar

# 3. Copy the archive into the KIND node
docker cp /tmp/source-garmin.tar airbyte-abctl-control-plane:/root/source-garmin.tar

# 4. Import it into the k8s.io containerd namespace (required by Kubernetes)
docker exec airbyte-abctl-control-plane \
  ctr -n k8s.io images import /root/source-garmin.tar
```

Then in the Airbyte UI (`http://localhost:8000`):

- **Settings → Sources → New connector**
- Docker repository name: `source-garmin`
- Docker image tag: `dev`

> **Note:** Steps 2–4 must be repeated every time you rebuild the image. Consider
> setting up a local Docker registry (`docker run -d -p 5000:5000 registry:2`) to
> avoid the tar export/import cycle during iterative development.

---

## Running tests

```bash
pip install -r requirements-dev.txt

# Unit tests (no Garmin credentials required)
pytest unit_tests/ -v

# Integration tests (requires real credentials in secrets/config.json)
pytest integration_tests/ -v -s
```

---

## Project structure

```
airbyte-source-garmin/
├── main.py                        # Airbyte CLI entrypoint (spec/check/discover/read)
├── setup.py
├── requirements.txt
├── requirements-dev.txt
├── Dockerfile
│
├── source_garmin/
│   ├── config.py                  # ConnectorConfig (Pydantic) + SPEC builder
│   ├── auth.py                    # GarminAuth: SSO login, session persistence, retry
│   ├── source.py                  # SourceGarmin: check / discover / read orchestrator
│   └── streams/
│       ├── base.py                # GarminStream abstract base class
│       ├── activities.py          # ActivitiesStream (FULL_REFRESH + INCREMENTAL)
│       ├── daily_health.py        # DailyHealthStream (FULL_REFRESH + INCREMENTAL)
│       └── calendar_events.py     # CalendarEventsStream (FULL_REFRESH)
│
├── unit_tests/
│   ├── fixtures/                  # JSON fixtures mirroring real Garmin API responses
│   ├── test_auth.py               # 14 tests — session persistence, retry logic
│   └── test_streams.py            # 85 tests — field mapping, conversions, state, protocol
│
└── integration_tests/
    └── test_source.py             # End-to-end tests (require real credentials)
```

---

## Architecture decisions

Non-obvious technical choices are recorded in [DECISIONS.md](DECISIONS.md).
Known issues and deferred fixes are tracked in [KNOWN_BUGS.md](KNOWN_BUGS.md).

---

## Authentication note

Garmin Connect has **no official public API**. This connector authenticates using
[`garminconnect`](https://github.com/cyberjunky/python-garminconnect), which reverse-engineers the Garmin SSO login flow.

To avoid triggering Garmin's rate limiter on every sync, the session token is serialised to `session_file_path` after the first successful login and reloaded on subsequent runs. Only an expired or missing token triggers a new SSO login.
