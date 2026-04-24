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

---

## ADR-008 — `load_config()` uses `json.load() + **raw` instead of the built-in JSON settings source

**Status**: Accepted  
**Step**: 2 (Config)

### Context
`pydantic-settings` v2 ships a `JsonConfigSettingsSource` that can read a JSON file
automatically as part of the settings resolution chain. We could have wired it up
in `model_config` and avoided `load_config()` entirely.

### Decision
Use an explicit `json.load(config_path)` + `ConnectorConfig(**raw)` pattern in a
standalone `load_config()` helper instead.

### Reasons
1. **File path comes from the CLI** — Airbyte passes `--config /path/to/file` as a
   runtime argument. The pydantic-settings built-in JSON source requires the path to
   be known at *class definition time* (baked into `model_config`), not at
   instantiation time. Wiring the CLI argument through would require either a global
   variable or a factory pattern more complex than the simple helper we have.
2. **Explicit is clearer** — `load_config(path)` is a single, obvious call site.
   A reader can follow the data flow without knowing how `SettingsConfigDict`'s
   sources are prioritised.
3. **env-var override still works** — because we still subclass `BaseSettings`, the
   `GARMIN_*` environment variable override (see `env_prefix`) remains available for
   Docker deployments, even though the JSON file is loaded manually.

### Trade-offs
- We lose the automatic layering (env vars > JSON file > defaults) that
  `JsonConfigSettingsSource` provides for free. In practice, `GARMIN_*` env vars
  still override defaults via `BaseSettings`, but they do *not* override values
  from the JSON file — the `**raw` unpack takes precedence. This is acceptable
  because Airbyte always provides a complete config file.

---

## ADR-009 — `read_records()` receives an authenticated client, not a config

**Status**: Accepted  
**Step**: 4 (Base stream)

### Context
Stream implementations need to call the Garmin API. They could either receive
the full `ConnectorConfig` and instantiate `GarminAuth` themselves, or receive
an already-authenticated `garminconnect.Garmin` client from the caller.

### Decision
`read_records()` accepts a `garminconnect.Garmin` client as its first argument.
Authentication is the responsibility of `source.py`, not the streams.

### Reasons
1. **Separation of concerns** — streams are pure data-fetching logic; auth is
   infrastructure. Mixing them would make each stream harder to read and test.
2. **Testability** — mocking a `garminconnect.Garmin` client in unit tests is
   a one-liner (`MagicMock()`). Mocking the full auth flow would require patching
   multiple layers.
3. **Single auth instance** — `source.py` creates one client and passes it to all
   streams, avoiding redundant login attempts or session file races.

---

## ADR-010 — `read()` is a generator (yields records one at a time)

**Status**: Accepted  
**Step**: 4 (Base stream)

### Context
`read()` could accumulate all records into a list and return it, or yield each
record as it is produced.

### Decision
`read()` (and `read_records()` in all stream implementations) are generators —
they `yield` individual Airbyte message dicts.

### Reasons
1. **Memory** — yielding records one at a time means the connector never holds
   all records in memory simultaneously. For a Garmin account with years of daily
   health data, buffering everything would be wasteful.
2. **Streaming protocol** — Airbyte reads connectors by consuming stdout line by
   line. A generator maps naturally onto this: each `yield` becomes one JSON line
   printed by `main.py`, with no intermediate list allocation.

---

## ADR-011 — `_compute_start_date()` accepts `today` as a parameter

**Status**: Accepted  
**Step**: 4 (Base stream)

### Context
The start date of the fetch window depends on "today". Hardcoding `date.today()`
inside the method makes it impossible to test deterministically.

### Decision
`_compute_start_date()` accepts `today: date` as an explicit argument, injected
by the `read()` method which calls `date.today()` once at the top of the run.

### Reasons
- Unit tests can pass a fixed `today` value and assert exact date windows without
  time-dependent flakiness.
- `date.today()` is called exactly once per sync run, which is the correct
  behaviour — all streams in a single run share the same reference date.

---

## ADR-012 — pandas for field transformation even on small datasets

**Status**: Accepted  
**Step**: 5 (Activities stream)

### Context
The activities stream receives a list of raw Garmin dicts and needs to apply unit
conversions and sanity checks. Plain Python dict comprehensions could do this just
as well for a dataset of hundreds of rows.

### Decision
Load the raw list into a pandas DataFrame, apply transformations column by column,
then yield row by row.

### Reasons
1. **CLAUDE.md mandate** — the spec explicitly requires pandas for all data
   manipulation.
2. **Column-wise operations are the pandas idiom** — applying a conversion to an
   entire column at once (`df["distance_km"] = df["distance_m"] / 1000`) is both
   more readable and more representative of real ETL work than looping over dicts.
3. **Null handling** — pandas provides `pd.notna()`, `errors="coerce"` in
   `pd.to_datetime()`, and nullable dtypes (`pd.Int64Dtype()`), which turn what
   would be defensive try/except boilerplate in plain Python into a declarative
   one-liner.

### Trade-offs
- Introduces a pandas import for what could be a pure-Python transformation.
  Acceptable given the explicit project constraint and the pedagogical value.

---

## ADR-013 — `pd.Int64Dtype()` for nullable integer columns

**Status**: Accepted  
**Step**: 5 (Activities stream)

### Context
pandas represents missing values as `NaN`, which is a float. When a column has
type `int64` and contains even one `NaN`, pandas silently upcasts the entire column
to `float64` — so `activity_id=12345678` becomes `12345678.0` in the output dict.
The JSON schema declares these fields as `"type": ["integer", "null"]`, so floats
are incorrect.

### Decision
Cast integer columns that may contain nulls (`activity_id`, `avg_heart_rate`,
`max_heart_rate`, `calories`) to `pd.Int64Dtype()` (pandas nullable integer) after
all transformations are applied.

### Reasons
- `pd.Int64Dtype()` stores genuine integers alongside `pd.NA` (not `NaN`), so
  `.to_dict()` yields `12345678` (int) and `None`, never `12345678.0` (float).
- This matches the JSON Schema declaration and avoids type mismatches at the
  destination (e.g. BigQuery would reject `158.0` for an INT64 column).

### How to spot the bug without this fix
```python
# Without pd.Int64Dtype():
df["avg_heart_rate"] = df["avg_hr_raw"].apply(lambda v: int(v) if v else None)
# → column dtype becomes float64 because NaN forces the upcast
# → to_dict() returns 158.0 instead of 158
```

---

## ADR-014 — `yield from stream.read()` to delegate generators in `source.py`

**Status**: Accepted  
**Step**: 6 (Main source)

### Context
`SourceGarmin.read()` needs to forward every message produced by each stream's
`read()` generator to its own caller (ultimately `main.py`).

### Decision
Use `yield from stream.read(...)` instead of an explicit loop.

### Reasons
`yield from` is Python's generator delegation syntax — it transparently forwards
every value yielded by the inner generator, propagates exceptions, and handles
`send()` / `throw()` correctly. The explicit alternative:

```python
for message in stream.read(...):
    yield message
```

is functionally equivalent but slightly more verbose and slightly less efficient
(each value crosses an extra stack frame). `yield from` signals intent clearly:
"this function is a pass-through generator for this sub-generator".

---

## ADR-015 — Single auth client per run, shared across all streams

**Status**: Accepted  
**Step**: 6 (Main source)

### Context
`SourceGarmin.read()` iterates over multiple streams. Each stream could
instantiate its own `GarminAuth` and authenticate independently.

### Decision
`GarminAuth` is instantiated once in `SourceGarmin.read()` and the resulting
authenticated client is passed to every stream.

### Reasons
1. **Rate-limit safety** — Garmin rate-limits logins aggressively. Creating one
   client per stream would multiply login attempts and risk a temporary account
   lock on the first sync.
2. **Performance** — Even with session restore (no network call), instantiating
   `GarminAuth` multiple times is wasteful. One call, one client.
3. **Consistency** — All streams in a single run operate against the same
   authenticated session, so there is no risk of one stream seeing a token
   refresh that another stream has not.

### Trade-offs
If a session expires *mid-run* (unlikely but possible for very long syncs),
the shared client will start raising auth errors for all subsequent streams.
The fix would be to add a re-authentication step inside `GarminStream.read()`,
but this edge case is not worth the added complexity at this stage.
