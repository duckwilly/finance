#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Error: required command '$1' not found in PATH" >&2
    exit 1
  fi
}

step() {
  echo
  echo "==> $1"
}

START_SERVER="${QUICKSTART_START_SERVER:-1}"

require_command docker

if [[ ! -f .env ]]; then
  if [[ -f .env.example ]]; then
    step "Creating .env from .env.example"
    cp .env.example .env
  else
    echo "Warning: .env not found and .env.example missing; continuing with defaults" >&2
  fi
fi

if [[ -f .env ]]; then
  step "Loading environment variables from .env"
  # shellcheck disable=SC1091
  set -a
  source .env
  set +a
fi

PYTHON_BIN="${PYTHON:-python3}"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "Error: unable to locate python interpreter '$PYTHON_BIN'" >&2
  exit 1
fi

step "Creating virtual environment (.venv)"
if [[ ! -d .venv ]]; then
  "$PYTHON_BIN" -m venv .venv
else
  echo ".venv already exists, reusing"
fi

# shellcheck disable=SC1091
source .venv/bin/activate

step "Installing Python dependencies"
pip install -q -U pip
pip install -q -r requirements.txt

step "Starting MariaDB with Docker Compose"
docker compose -f docker/docker-compose.yaml up -d

step "Waiting for the database to become available"
python - <<'PY'
import time

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from app.config import get_settings

settings = get_settings()
engine = create_engine(settings.database.sqlalchemy_url, pool_pre_ping=True, future=True)

deadline = time.monotonic() + 180
while True:
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except OperationalError:
        if time.monotonic() >= deadline:
            raise SystemExit("Database did not become ready within 180 seconds")
        time.sleep(2)
    else:
        break
PY

step "Clearing existing database data"
python scripts/clear_database.py

step "Applying database schema"
python - <<'PY'
from pathlib import Path

from sqlalchemy import create_engine, text

from app.config import get_settings


def iter_statements(path: Path):
    buffer = []
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        buffer.append(line)
        if stripped.endswith(";"):
            statement = "\n".join(buffer).rstrip(";")
            buffer = []
            yield statement
    if buffer:
        yield "\n".join(buffer)


settings = get_settings()
engine = create_engine(settings.database.sqlalchemy_url, future=True)
schema_path = Path("sql/schema.sql")

if not schema_path.exists():
    raise SystemExit(f"Schema file not found: {schema_path}")

with engine.begin() as connection:
    for raw_statement in iter_statements(schema_path):
        connection.execute(text(raw_statement))
PY

step "Running database smoketest"
python scripts/db_smoketest.py

step "Clearing existing seed data"
python - <<'PY'
import os
from pathlib import Path

seed_dir = Path("data/seed")
if seed_dir.exists():
    for file in seed_dir.glob("*.csv"):
        file.unlink()
        print(f"Removed: {file}")
    print("Cleared existing seed data files")
else:
    print("No existing seed data to clear")
PY

step "Fetching historical stock prices and FX rates"
python scripts/fetch_stock_prices.py

step "Generating seed CSV data"
python scripts/gen_seed_data.py --seed $(date +%s)

step "Loading CSV data into MariaDB"
python scripts/load_csvs.py

if [[ "${START_SERVER}" == "1" ]]; then
  require_command uvicorn
  step "Quickstart complete"
  step "Starting FastAPI development server"
  UVICORN_HOST="${QUICKSTART_HOST:-0.0.0.0}"
  UVICORN_PORT="${QUICKSTART_PORT:-8000}"
  UVICORN_LOG_LEVEL="${QUICKSTART_LOG_LEVEL:-info}"

  cleanup() {
    if [[ -n "${UVICORN_PID:-}" ]]; then
      kill "${UVICORN_PID}" >/dev/null 2>&1 || true
      wait "${UVICORN_PID}" 2>/dev/null || true
    fi
  }

  trap cleanup EXIT INT TERM
  uvicorn app.main:app --host "${UVICORN_HOST}" --port "${UVICORN_PORT}" --log-level "${UVICORN_LOG_LEVEL}" --reload &
  UVICORN_PID=$!
  echo "FastAPI application running at http://${UVICORN_HOST}:${UVICORN_PORT} (PID ${UVICORN_PID})."
  echo "Press Ctrl+C to stop the server."
  wait "${UVICORN_PID}"
else
  step "Quickstart complete"
  echo "Server startup skipped (QUICKSTART_START_SERVER=${START_SERVER})."
fi
