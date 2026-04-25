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

## ADR-007 — `calendar.py` renamed to `calendar_events.py`

**Status**: Accepted  
**Step**: 12 (Bug fixes) — supersedes the pending note from Step 11

### Context
The scaffolded file is `source_garmin/streams/calendar.py` but CLAUDE.md specifies
`calendar_events.py`. The name `calendar` also shadows Python's built-in `calendar`
standard-library module. See KNOWN_BUGS.md → KB-001.

### Decision
Renamed via `git mv` in Step 12. Imports updated in `source.py` and `test_streams.py`.

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

---

## ADR-016 — `json.dumps(default=str)` as a serialisation safety net in `_emit()`

**Status**: Accepted  
**Step**: 7 (Entrypoint)

### Context
Every Airbyte message is serialised with `json.dumps()` in `_emit()`. Standard
`json.dumps()` raises `TypeError` if any value is not JSON-serialisable (e.g. a
`datetime`, `date`, or `Decimal` object that slipped through the transformation
layer).

### Decision
Pass `default=str` to `json.dumps()` in `_emit()`.

### Reasons
- If a non-serialisable value reaches `_emit()`, the entire sync fails with an
  opaque `TypeError` rather than a meaningful error message. `default=str` converts
  the value to its string representation and logs it, allowing the sync to complete
  and making the issue visible in the destination.
- The proper fix is to ensure all field types are JSON-safe before yielding
  (handled in stream transformations), but `default=str` is a last-resort guard
  that prevents a type edge case from crashing a full sync.

### Trade-offs
- A value serialised via `str()` (e.g. `"2024-03-15"` from a `date` object) may
  look correct in the destination but silently bypass the declared schema type.
  Acceptable as a fallback; not acceptable as a primary strategy.

---

## ADR-017 — `check` exits with code 0 even when status is FAILED

**Status**: Accepted  
**Step**: 7 (Entrypoint)

### Context
When credentials are wrong, `check` emits a `CONNECTION_STATUS: FAILED` message.
One might expect the process to exit with code 1 to signal failure to the caller.

### Decision
`check` always exits with code 0, regardless of the connection status.

### Reasons
This is a requirement of the Airbyte protocol. The `check` command communicates
success or failure exclusively through the `CONNECTION_STATUS` message on stdout —
the exit code is not inspected by the Airbyte platform for this command. Exiting
with code 1 on a FAILED check would be non-standard and could break integrations
that follow the protocol strictly.

Contrast this with `read`, where a fatal exception does warrant `sys.exit(1)`,
because an incomplete read with exit code 0 would falsely appear as a successful
sync in the Airbyte UI.

---

## ADR-018 — Unit tests mock at the network boundary only

**Status**: Accepted  
**Step**: 8 (Unit tests)

### Context
Unit tests for streams need to avoid real Garmin API calls. The question is *how
deep* to mock: you could mock `garminconnect.Garmin`, or you could go further and
mock individual methods inside the stream classes (e.g. `_transform()`,
`_normalize_raw()`), or you could mock pandas itself.

### Decision
Mock only `garminconnect.Garmin` (the external network boundary). All
transformation logic — `_normalize_raw()`, `_transform()`, `_check_hr()`,
`_speed_to_pace()`, etc. — runs for real against fixture data.

### Reasons
1. **Tests catch real bugs** — the most common bugs in a connector live in field
   mapping, unit conversions, and sanity checks. If those methods are mocked out,
   the tests verify nothing meaningful.
2. **Fixture JSON files as living documentation** — fixture files in
   `unit_tests/fixtures/` mirror the real Garmin API response shape. When the
   real API changes, the fixtures can be updated from a real response and the
   tests will immediately reveal which transformations break.
3. **Transformation code is pure** — `_transform()` takes a DataFrame and returns
   a DataFrame with no side effects. Pure functions are trivial to test without
   mocking by just passing real DataFrames built from fixture data.

### Trade-offs
- Tests are slightly slower than if all logic were mocked (a pandas DataFrame is
  created per test). In practice the suite runs in under one second for the
  current volume of tests.
- If `garminconnect.Garmin`'s interface changes (e.g. a renamed method), the mock
  will silently continue to pass. Integration tests (Step 9+) are the backstop
  for this class of regression.

---

## ADR-019 — DailyHealthStream: one API call per day (get_user_summary)

**Status**: Accepted  
**Step**: 10 (DailyHealthStream)

### Context
Garmin health data (steps, sleep, stress, body battery, HRV) comes from several
underlying sensors and is aggregated into a per-day summary. The `garminconnect`
library does not expose a single batch endpoint that returns all these fields for
a date range in one call.

### Decision
Call `client.get_user_summary(date)` once per calendar day in the fetch window.
The response includes all required fields plus the nested `lastNight` object
(sleep data), which `_normalize_raw()` flattens before loading into pandas.

### Reasons
1. **Single call, complete data** — `get_user_summary` returns all required
   fields in one response, avoiding the complexity of joining separate sleep,
   stress, and steps endpoints.
2. **Graceful partial failures** — a failed call for a single day is logged
   as a warning and skipped rather than aborting the whole stream. Days where
   the user did not sync their watch (common) return a 404 from Garmin.
3. **Testable** — mocking `get_user_summary` with `side_effect=[item1, item2]`
   makes per-day sequential assertions clean and deterministic.

### Trade-offs
- For a 30-day lookback, this makes 30 sequential API calls. Garmin does not
  appear to rate-limit this endpoint as aggressively as the login endpoint, but
  a future optimisation could batch calls if needed.

---

## ADR-020 — CalendarEventsStream: ISO week iteration with forward-looking window

**Status**: Accepted  
**Step**: 11 (CalendarEventsStream)

### Context
The Garmin calendar API exposes a week-granularity endpoint
(`get_calendar_week(year, week)`) with no batch or date-range variant.
Calendar events represent upcoming races and training events — querying only
the past `lookback_days` would miss future events that exist in the user's
calendar at sync time.

### Decision
Iterate over ISO weeks from `start_date` to `today + 365 days`, overriding the
base class `end_date` inside `read_records()`. A `set` of seen event IDs
deduplicates events that appear in two consecutive week responses (can happen
when an event falls on a week boundary — e.g. a Sunday event appears in both
the ISO week that ends on that Sunday and the next week's response in some API
implementations).

### Reasons
1. **Forward-looking window** — races are registered months in advance. A
   365-day forward window ensures they are captured on every FULL_REFRESH sync.
2. **FULL_REFRESH only** — calendar events are mutable (can be cancelled,
   renamed, rescheduled). Re-fetching everything on every sync is the safest
   approach; incremental state would risk silently missing changes.
3. **Deduplication via set** — O(1) lookup, negligible overhead for typical
   calendar sizes (tens of events per year).

### Trade-offs
- The 365-day forward window is hardcoded (`_FORWARD_DAYS = 365`). A future
  improvement could make this configurable via `ConnectorConfig`.
- Week iteration means up to ~55 API calls per sync (52 weeks + partial weeks
  at boundaries). This is acceptable for the current use case.

---

## ADR-021 — STATE message is only emitted in incremental sync mode

**Status**: Accepted  
**Step**: 12 (Bug fixes — supersedes the undecided behaviour documented in KB-008)

### Context
`GarminStream.read()` tracks the highest cursor value seen across all records and
emits a STATE message at the end of the run. In the original implementation the
guard was:

```python
if self.cursor_field and latest_cursor:
    yield self._make_state_message(...)
```

This emitted STATE even during `full_refresh` runs. A `full_refresh` sync causes
Airbyte to truncate and reload the destination table — the destination ignores any
saved state and the next run always starts from scratch. Emitting STATE in
`full_refresh` was therefore technically harmless but created misleading output and
caused a unit test to need a comment justifying unexpected behaviour.

### Decision
Guard the STATE emit with an additional `sync_mode == "incremental"` check:

```python
if sync_mode == "incremental" and self.cursor_field and latest_cursor:
    yield self._make_state_message(...)
```

### Reasons
1. **Protocol correctness** — the Airbyte protocol does not forbid STATE in
   full\_refresh, but emitting it implies the destination should use it, which is
   misleading given that full\_refresh discards state by definition.
2. **Test hygiene** — the original test for `test_no_state_emitted_for_full_refresh`
   could not assert `state_msgs == []` and instead only asserted that records exist,
   with a comment documenting the unexpected behaviour. The fix allows a clean,
   direct assertion.
3. **Consistency** — `CalendarEventsStream` has no cursor and never emits STATE.
   `ActivitiesStream` and `DailyHealthStream` should behave analogously when run in
   full\_refresh mode.

### Trade-offs
- None: the change is strictly narrowing (fewer messages emitted), compatible with
  all Airbyte destination connectors, and covered by the updated unit test.

---

## ADR-022 — Shared `retry_on_429` utility replaces duplicated retry loops



**Status**: Accepted  
**Step**: v0.1.1 (Bug fix — resolves KB-006)

### Context
The initial implementation contained retry-on-429 logic only in
`GarminAuth._login_with_retry()`.  Stream `read_records()` methods made API calls
without any retry protection: activities would crash on 429, while daily_health and
calendar_events would silently skip affected records (the exception was swallowed by
the `except Exception` log-and-continue handler).

### Decision
Extract the backoff loop into `source_garmin/utils.py: retry_on_429(fn, delays)`.
`GarminAuth._login_with_retry()` is simplified to delegate to it.  All three
stream `read_records()` methods wrap their Garmin API calls with it.

`_RETRY_DELAYS` (the backoff schedule) is defined once in `utils.py` and imported
wherever it is needed — there is no duplication of the magic numbers.

### Reasons
1. **Single source of truth** — the backoff schedule (30 s, 60 s, 120 s) exists in
   one place.  Changing it in `utils.py` updates all callers automatically.
2. **Correctness** — previously a 429 on a per-day health summary call or a
   per-week calendar call was silently logged as a warning and the record was
   dropped.  Now those calls retry before giving up.
3. **Testability** — `retry_on_429` is a pure function that takes a callable and a
   delay list.  It is straightforward to unit-test in isolation in `test_utils.py`
   without any stream or auth context.

### Trade-offs
- The `lambda` closures used to pass arguments to `retry_on_429` (e.g.
  `lambda: client.get_activities_by_date(start, end)`) capture variables from the
  enclosing scope by reference.  In a loop this can cause the classic
  "late-binding closure" bug.  All three stream lambdas are created inside a
  function call (not inside a loop variable assignment), so the captured values
  are stable — but reviewers should be aware of this pattern.
- `_login_with_retry()` no longer logs "successful on attempt X/Y" because
  `retry_on_429` does not expose the attempt number to the caller.  The retry
  warning logs from `retry_on_429` itself are sufficient.

---

## ADR-023 — Session validation uses `connectapi("/userprofile-service/socialProfile")` instead of `get_full_name()`

**Status**: Accepted  
**Step**: v0.1.2 (Bug fix — resolves KB-012)

### Context
`_try_load_session()` needed a "cheap" network call to confirm the restored OAuth
token was still valid. The original choice was `client.get_full_name()`. In
`garminconnect` 0.3.x, `get_full_name()` only returns the cached `self.full_name`
attribute — it makes no network call and never raises. Calling it after
`client.client.load(path)` always returned `None` silently, so every restored
session was considered valid regardless of token state. Worse, `display_name`
(a separate attribute) remained `None`, causing `get_user_summary()` to fail via
`_require_display_name()` with no error surfaced to the user.

### Decision
Replace `client.get_full_name()` with:

```python
profile = client.connectapi("/userprofile-service/socialProfile")
client.display_name = profile.get("displayName", client.username)
client.full_name = profile.get("fullName")
```

### Reasons
1. **Real network validation** — `connectapi()` makes an actual HTTPS request; an
   expired or revoked token raises an exception that `_try_load_session()` catches
   and converts to `return False`.
2. **Profile initialisation** — the social profile response sets `display_name` and
   `full_name` in one call, matching exactly what the full SSO login flow does.
   All subsequent endpoints that call `_require_display_name()` (e.g.
   `get_user_summary()`) will work correctly after a session restore.
3. **Single source of truth** — the social profile endpoint is already what
   `garminconnect` uses internally after a successful login; using the same endpoint
   in session restore ensures both paths produce an identically-initialised client.

### Trade-offs
- One extra API call per session restore (previously zero calls in the load path).
  The cost is negligible: session restore happens at most once per connector run,
  and the call is lighter than a full activity or health fetch.
- The `/userprofile-service/socialProfile` endpoint path is not part of the public
  `garminconnect` API surface. If Garmin changes this URL, `_try_load_session()`
  will return `False` on every run (triggering a fresh SSO login) — a graceful
  degradation rather than a crash.
