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

require_command docker

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

step "Generating seed CSV data"
python scripts/gen_seed_data.py

step "Loading CSV data into MariaDB"
python scripts/load_csvs.py

step "Running database smoketest"
python scripts/db_smoketest.py

step "Starting FastAPI admin dashboard (press Ctrl+C to stop)"
exec uvicorn app.web.app:app --reload
