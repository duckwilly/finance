# app/core/templates.py
from __future__ import annotations
from fastapi.templating import Jinja2Templates

from app.core.formatting import humanize_number, humanize_currency

# Single shared templates environment
templates = Jinja2Templates(directory="app/templates")

templates.env.filters["humanize_number"] = humanize_number
templates.env.filters["humanize_currency"] = humanize_currency

templates.env.globals["humanize_number"] = humanize_number
templates.env.globals["humanize_currency"] = humanize_currency