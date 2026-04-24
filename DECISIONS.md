# DECISIONS.md — Architectural Decision Log

Every non-obvious technical choice is recorded here so that future contributors
(and interview panels) can understand *why* the code is shaped the way it is.

---

## ADR-001 — Raw Airbyte protocol instead of the Python CDK

**Status**: Accepted  
**Step**: 1 (Setup)

### Context
Airbyte ships a [Python CDK](https://github.com/airbytehq/airbyte/tree/master/airbyte-cdk/python)
that provides base classes (`Source`, `Stream`, `HttpStream`, …) that handle
much of the protocol boilerplate automatically.

### Decision
We implement the Airbyte protocol from scratch, emitting raw JSON messages to
stdout without depending on the CDK package.

### Reasons
1. **Learning goal** — manually implementing `SPEC`, `CHECK`, `CATALOG`, `RECORD`,
   and `STATE` messages teaches the actual protocol rather than hiding it behind
   framework abstractions.
2. **Garmin-specific auth** — the CDK's `HttpStream` assumes standard OAuth or
   API-key flows. Garmin uses SSO scraping via `garth`, which does not map cleanly
   onto CDK abstractions; fighting the framework would add more complexity than it
   removes.
3. **Portfolio value** — demonstrating that you understand the protocol at the
   message level is more compelling in a Data Engineering interview than showing
   you can extend framework base classes.

### Trade-offs
- More boilerplate in `main.py` and `source.py` (we write the message serialisation
  ourselves).
- The connector cannot be trivially loaded into the Airbyte UI's "custom connector"
  flow that expects CDK connectors — however, it can still be run as a Docker
  image, which is the standard deployment path.

---

## ADR-002 — `pydantic-settings` v2 for connector config

**Status**: Accepted  
**Step**: 1 (Setup)

### Context
The connector receives its config as a JSON file path via `--config /secrets/config.json`.
We need to parse, validate, and type-coerce that file into a Python object.

### Decision
Use `pydantic-settings` (`BaseSettings`) to declare the config schema.

### Reasons
1. **Single source of truth** — the same Pydantic model drives both runtime
   validation and the `SPEC` message output (we generate the JSON Schema from
   the model).
2. **Type coercion** — Pydantic automatically converts `"30"` → `30` for
   integer fields, saving defensive parsing code.
3. **Secret masking** — `SecretStr` hides the password from log output with zero
   extra code.

---

## ADR-003 — `loguru` instead of standard `logging`

**Status**: Accepted  
**Step**: 1 (Setup)

### Context
The Airbyte protocol requires `LOG` messages emitted to stdout in JSON format.
The connector also needs human-readable coloured output during local development.

### Decision
Use `loguru` as the sole logging library.

### Reasons
1. **Zero configuration** — `loguru` works out of the box with coloured, levelled
   output; no `logging.basicConfig()` or handler setup needed.
2. **Easy JSON sink** — adding a JSON sink for Airbyte `LOG` messages is a
   one-liner: `logger.add(sys.stdout, serialize=True)`.
3. **CLAUDE.md mandate** — the project specification explicitly requires `loguru`.

---

## ADR-004 — `pandas` only for data manipulation

**Status**: Accepted  
**Step**: 1 (Setup)

### Context
Field mapping and sanity checks require transforming raw Garmin API responses
(nested dicts) into flat records aligned with the stream schemas.

### Decision
Use `pandas` exclusively. `polars` is explicitly forbidden by CLAUDE.md.

### Reasons
- `pandas` is the industry standard for this kind of light ETL work in Python.
- Most Data Engineering interview panels expect `pandas` fluency.
- `polars` is faster for large datasets but Garmin data volumes (hundreds of
  activities, daily health rows) do not justify the added dependency or the
  learning curve.

---

## ADR-005 — Session file persistence to avoid Garmin login rate-limits

**Status**: Accepted  
**Step**: 1 (Setup) — implementation deferred to Step 3 (Auth)

### Context
Garmin does not provide an official API. The `garminconnect` library authenticates
via SSO scraping. Garmin aggressively rate-limits repeated logins (HTTP 429,
temporary account lock).

### Decision
After the first successful login, serialise the OAuth token to a JSON file via
`client.garth.dump(path)`. On subsequent runs, load the token via
`client.garth.load(path)` and skip the login entirely unless the token has expired.

### Reasons
- Avoids hitting Garmin's rate limiter during development (multiple test runs per
  day would trigger a ban).
- Required in Docker: the session file is mounted as a volume so it survives
  container restarts.

---

## ADR-006 — Separate `requirements.txt` and `requirements-dev.txt`

**Status**: Accepted  
**Step**: 1 (Setup)

### Decision
Runtime dependencies live in `requirements.txt`; test/dev tools live in
`requirements-dev.txt`, which includes `requirements.txt` via `-r requirements.txt`.

### Reasons
- The Docker image only installs `requirements.txt`, keeping the image lean.
- A single `pip install -r requirements-dev.txt` sets up a complete local
  environment (including runtime deps) without duplication.

---

## ADR-007 — `calendar.py` renamed to `calendar_events.py` (pending)

**Status**: Pending  
**Step**: 11 (Calendar events stream)

### Context
The scaffolded file is `source_garmin/streams/calendar.py` but CLAUDE.md specifies
`calendar_events.py`. See KNOWN_BUGS.md → KB-001.

### Decision
The file will be renamed (not patched) when implementing the calendar events stream
in Step 11.
