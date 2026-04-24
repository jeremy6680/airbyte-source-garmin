# KNOWN_BUGS.md — Open Issues & Mismatches

This file tracks known issues, naming mismatches, and deferred fixes so that
nothing falls through the cracks across development steps.

---

## KB-001 — `calendar.py` should be `calendar_events.py`

**Severity**: Low (build-time, not runtime)  
**Introduced**: Initial scaffold  
**Target fix**: Step 11 (Calendar events stream)

### Description
The scaffolded file is `source_garmin/streams/calendar.py`, but CLAUDE.md specifies
the canonical name `source_garmin/streams/calendar_events.py`.

The name `calendar` also shadows Python's built-in `calendar` standard-library
module, which could cause confusing `ImportError` messages if any dependency
imports it.

### Fix plan
When implementing the calendar events stream (Step 11), delete `calendar.py` and
create `calendar_events.py` from scratch. Update the import in
`source_garmin/streams/__init__.py` accordingly.

### Affected files
- `source_garmin/streams/calendar.py` — to be deleted
- `source_garmin/streams/calendar_events.py` — to be created
- `source_garmin/streams/__init__.py` — import to be updated
- `source_garmin/source.py` — stream registration to be updated

---

## KB-002 — `source_garmin/manifest.yaml` is an empty leftover

**Severity**: Cosmetic  
**Introduced**: Initial scaffold  
**Target fix**: Step 6 (Main source) or cleanup pass

### Description
`source_garmin/manifest.yaml` was created by the initial scaffold but is empty.
It is only meaningful for Airbyte's declarative connector builder and has no role
in this low-level Python connector.

### Fix plan
Delete the file in the cleanup pass after Step 6, once the connector structure
is confirmed stable.

---

## KB-003 — `metadata.yaml` is an empty leftover

**Severity**: Cosmetic  
**Introduced**: Initial scaffold  
**Target fix**: Step 9 (Docker) or cleanup pass

### Description
`metadata.yaml` at the project root is empty. Airbyte uses this file in its
connector registry to declare connector metadata (name, icon, version, etc.).
It is not required for local or Docker-based operation.

### Fix plan
Either fill it with valid metadata when packaging the connector for Docker (Step 9),
or delete it if we are not targeting the Airbyte connector registry.

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
