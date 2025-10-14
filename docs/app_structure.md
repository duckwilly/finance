# Application Package Overview

This document walks through the current contents of the `app/` package, explains the role of each module, and notes any redundancies or opportunities to reorganise the code.

## Top-level module: `app/__init__.py`
Exports the shared logging helper so callers can simply import `get_logger` from `app` without knowing about the deeper logging package. 【F:app/__init__.py†L1-L4】

## Configuration: `app/config`
- `__init__.py` re-exports the settings helpers for convenient imports. 【F:app/config/__init__.py†L1-L4】
- `settings.py` loads environment variables (optionally via `.env`) to produce `Settings` and `DatabaseSettings` dataclasses, exposing the SQLAlchemy URL that the rest of the system uses. 【F:app/config/settings.py†L1-L74】

### Observations
The settings layer is cohesive and already shared by scripts and the FastAPI stack. As the project grows, consider promoting `Settings` to Pydantic BaseSettings (or similar) for validation, but no immediate duplication exists.

## Database helpers: `app/db`
- `__init__.py` collects the engine and session helpers. 【F:app/db/__init__.py†L1-L12】
- `engine.py` wraps SQLAlchemy engine creation using configuration defaults and centralised logging. 【F:app/db/engine.py†L1-L34】
- `session.py` exposes a `sessionmaker` factory and a context-managed `session_scope` for scripts. 【F:app/db/session.py†L1-L36】

### Observations
The module provides a single place to configure SQLAlchemy; the FastAPI dependency reuses the same sessionmaker, so no redundant engine creation occurs. If async APIs arrive later, you may want a parallel `async_session.py` rather than overloading these functions.

## ETL utilities: `app/etl`
Currently only exposes `resolve_section`, which maps CSV categories to canonical sections using default rules. 【F:app/etl/__init__.py†L1-L13】

### Observations
The module is intentionally lightweight. If more ETL logic appears, grouping related transformations into submodules (e.g., `parsers/`, `rules/`) could prevent a single file from becoming monolithic.

## Logging subsystem: `app/log` and `app/logger.py`
- `app/log/__init__.py` initialises Rich-enhanced logging, queue handling, and exposes context/progress utilities. 【F:app/log/__init__.py†L1-L166】
- `app/logger.py` is a compatibility wrapper that re-exports the logging helpers from `app/log`. 【F:app/logger.py†L1-L21】

### Observations
Both modules are necessary today because existing scripts import `app.logger`. If you commit to the package name `app.log`, you could eventually fold `logger.py` into a deprecation shim that warns consumers to migrate, but it is harmless for now.

## Service layer: `app/services`
- `__init__.py` marks the package and documents the intent to host reusable business logic. 【F:app/services/__init__.py†L1-L4】
- `admin_dashboard.py` encapsulates SQL queries for the admin landing page, returning data classes that the template consumes. 【F:app/services/admin_dashboard.py†L1-L99】

### Observations
The service isolates SQL from the web layer, which is good for testing. As additional views arrive, consider splitting generic repository helpers versus specific dashboard aggregators to avoid a single service file growing too large.

## Web frontend: `app/web`
- `__init__.py` re-exports the FastAPI factory. 【F:app/web/__init__.py†L1-L5】
- `app.py` constructs the FastAPI application, wires the admin router, and defines the root redirect. 【F:app/web/app.py†L1-L20】
- `dependencies.py` builds a global `SessionFactory` and exposes a request-scoped dependency that yields SQLAlchemy sessions. 【F:app/web/dependencies.py†L1-L21】
- `routers/__init__.py` exposes router modules; currently only `admin`. 【F:app/web/routers/__init__.py†L1-L5】
- `routers/admin.py` defines the `/admin` route, injects a DB session, and renders the dashboard template. 【F:app/web/routers/admin.py†L1-L34】
- `templates/admin/dashboard.html` is the Jinja2 template that renders metrics and recent accounts with inline styling. 【F:app/web/templates/admin/dashboard.html†L1-L126】

### Observations
The structure mirrors typical FastAPI projects: dependencies isolated, routers grouped, templates under `templates/`. A couple of enhancements to consider:
1. **Template organisation:** Move inline CSS into a shared static asset once multiple pages exist to avoid duplication.
2. **Router packaging:** When more routers appear (e.g., `users`, `reports`), expose them individually and include them via the app factory for clarity. Alternatively, consider an `app/web/main.py` entrypoint for Uvicorn to import directly, reducing top-level exports.

## General opportunities
- Introduce a `domain/` (or `models/`) package if you start sharing dataclasses/DTOs across services and APIs, keeping `services` focused on orchestration.
- Add automated tests around service-layer SQL to catch regressions as the schema evolves.
- Document the logging setup inside `README.md` or this file once consumers need to customise log destinations.

Overall the current layout is modular and prepared for growth. The only mild redundancy is the `logger.py` shim; keep it until you can audit imports and switch everything to `app.log` directly.
