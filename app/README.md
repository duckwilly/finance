# Finance application

This directory contains the FastAPI application, domain models, services, and
templates that power the finance platform.

* `main.py` exposes the FastAPI application factory and mounts routers.
* `core/` houses configuration, logging, formatting, and security helpers.
* `routers/` defines dashboard and presentation routes.
* `models/`, `schemas/`, and `services/` hold SQLAlchemy models, Pydantic
  schemas, and business logic.
* `db/` contains database session/engine helpers and migration tooling.
* `templates/` and `static/` provide server-rendered UI and assets.
