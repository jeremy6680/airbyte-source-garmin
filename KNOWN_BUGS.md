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
