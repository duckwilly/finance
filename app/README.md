# Finance application scaffold

This directory contains the application code for the rebuilt finance platform.  At
this stage only skeleton modules exist so the codebase can be structured and
extended incrementally.

* `main.py` exposes the FastAPI application factory used by future routers.
* `core/` houses cross-cutting concerns such as configuration, logging and
  security helpers.
* `routers/`, `models/`, `schemas/`, `services/`, and `db/` contain placeholders
  for the respective application layers described in the project plan.
* `templates/` and `static/` provide locations for server-rendered assets.

Each module currently exports lightweight stubs that raise
`NotImplementedError`.  Replace these stubs with real implementations as new
features are developed.
