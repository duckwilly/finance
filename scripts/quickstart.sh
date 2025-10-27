#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

# Start overall timer
QUICKSTART_START=$SECONDS

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Error: required command '$1' not found in PATH" >&2
    exit 1
  fi
}

step() {
  # Print elapsed time for previous step if this isn't the first step
  if [[ -n "${STEP_START:-}" ]]; then
    STEP_ELAPSED=$((SECONDS - STEP_START))
    echo "    (completed in ${STEP_ELAPSED}s)"
  fi
  
  echo
  echo "==> $1"
  STEP_START=$SECONDS
}

# Default values
START_SERVER="${QUICKSTART_START_SERVER:-1}"
DETACH_SERVER="0"
SIMULATION_SIZE="medium"
SIMULATION_MONTHS=""
INDIVIDUALS=""
COMPANIES=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --size)
      SIMULATION_SIZE="$2"
      shift 2
      ;;
    --months)
      SIMULATION_MONTHS="$2"
      shift 2
      ;;
    --individuals)
      INDIVIDUALS="$2"
      shift 2
      ;;
    --companies)
      COMPANIES="$2"
      shift 2
      ;;
    --no-server)
      START_SERVER="0"
      shift
      ;;
    --detach-server)
      DETACH_SERVER="1"
      shift
      ;;
    --help)
      echo "Usage: $0 [OPTIONS]"
      echo ""
      echo "Options:"
      echo "  --size SIZE          Simulation size preset: small, medium, large (default: medium)"
      echo "  --months MONTHS      Number of months to simulate (overrides preset)"
      echo "  --individuals NUM    Number of individuals (overrides preset)"
      echo "  --companies NUM      Number of companies (overrides preset)"
      echo "  --no-server          Don't start the FastAPI server"
      echo "  --detach-server      Start the FastAPI server in the background and exit"
      echo "  --help               Show this help message"
      echo ""
      echo "Presets:"
      echo "  small:   50 individuals, 5 companies, 3 months"
      echo "  medium:  500 individuals, 50 companies, 12 months"
      echo "  large:   2000 individuals, 200 companies, 24 months"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

# Set parameters based on presets or use provided values
case "$SIMULATION_SIZE" in
  small)
    DEFAULT_INDIVIDUALS=50
    DEFAULT_COMPANIES=5
    DEFAULT_MONTHS=3
    ;;
  medium)
    DEFAULT_INDIVIDUALS=500
    DEFAULT_COMPANIES=50
    DEFAULT_MONTHS=12
    ;;
  large)
    DEFAULT_INDIVIDUALS=2000
    DEFAULT_COMPANIES=200
    DEFAULT_MONTHS=24
    ;;
  *)
    echo "Error: Invalid simulation size '$SIMULATION_SIZE'. Use: small, medium, large"
    exit 1
    ;;
esac

# Use provided values or defaults
FINAL_INDIVIDUALS="${INDIVIDUALS:-$DEFAULT_INDIVIDUALS}"
FINAL_COMPANIES="${COMPANIES:-$DEFAULT_COMPANIES}"
FINAL_MONTHS="${SIMULATION_MONTHS:-$DEFAULT_MONTHS}"

echo "Simulation configuration:"
echo "  Size preset: $SIMULATION_SIZE"
echo "  Individuals: $FINAL_INDIVIDUALS"
echo "  Companies: $FINAL_COMPANIES"
echo "  Months: $FINAL_MONTHS"
echo "  Start server: $([ "$START_SERVER" = "1" ] && echo "yes" || echo "no")"

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
# Compute start month so simulation ends in current month (portable date arithmetic)
START_YM=$(python3 -c "from datetime import date; today = date.today(); year = today.year; month = today.month - $FINAL_MONTHS + 1; 
while month <= 0: month += 12; year -= 1
print(f'{year}-{month:02d}')")
python scripts/gen_seed_data.py --seed $(date +%s) --individuals "$FINAL_INDIVIDUALS" --companies "$FINAL_COMPANIES" --months "$FINAL_MONTHS" --start "$START_YM"

step "Loading CSV data into MariaDB"
python scripts/load_csvs.py

# Print final step timing
if [[ -n "${STEP_START:-}" ]]; then
  STEP_ELAPSED=$((SECONDS - STEP_START))
  echo "    (completed in ${STEP_ELAPSED}s)"
  unset STEP_START  # Clear to prevent duplicate timing in server start step
fi

# Print overall timing
QUICKSTART_ELAPSED=$((SECONDS - QUICKSTART_START))
echo
echo "=========================================="
echo "Quickstart setup completed in ${QUICKSTART_ELAPSED}s ($(printf '%d:%02d' $((QUICKSTART_ELAPSED/60)) $((QUICKSTART_ELAPSED%60))))"
echo "=========================================="

if [[ "${START_SERVER}" == "1" ]]; then
  require_command uvicorn
  echo
  step "Starting FastAPI development server"
  UVICORN_HOST="${QUICKSTART_HOST:-0.0.0.0}"
  UVICORN_PORT="${QUICKSTART_PORT:-8000}"
  UVICORN_LOG_LEVEL="${QUICKSTART_LOG_LEVEL:-info}"

  if [[ "${DETACH_SERVER}" == "1" ]]; then
    uvicorn app.main:app --host "${UVICORN_HOST}" --port "${UVICORN_PORT}" --log-level "${UVICORN_LOG_LEVEL}" --reload &
    UVICORN_PID=$!
    echo "FastAPI application running at http://${UVICORN_HOST}:${UVICORN_PORT} (PID ${UVICORN_PID})."
    echo "Server started in background (detach mode)."
    echo "${UVICORN_PID}" > quickstart_uvicorn.pid
    echo "PID recorded in quickstart_uvicorn.pid for later teardown."
  else
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
  fi
else
  echo "Server startup skipped (QUICKSTART_START_SERVER=${START_SERVER})."
fi
