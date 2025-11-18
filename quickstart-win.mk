.PHONY: quickstart quickstart-small quickstart-medium quickstart-large quickstart-ci start start-prod venv deps db-up db-schema seed seed-reproducible load smoke clear-db clear-seed

SHELL := pwsh.exe
.SHELLFLAGS := -NoLogo -NoProfile -ExecutionPolicy Bypass -Command

PYTHON ?= py -3
VENV_DIR := .venv
VENV_ACTIVATE := $(VENV_DIR)\Scripts\Activate.ps1
COMPOSE_FILE := docker/docker-compose.yaml
SEED_DIR := data\seed

REQUIRE_VENV := if (-not (Test-Path '$(VENV_ACTIVATE)')) { Write-Error "Virtual environment not found. Run 'make -f quickstart-win.mk venv' first."; exit 1 }
ENSURE_BASH := if (-not (Get-Command bash -ErrorAction SilentlyContinue)) { Write-Error "bash is required for this target (install Git Bash or enable WSL)."; exit 1 }

quickstart:
	$(ENSURE_BASH); bash ./scripts/quickstart.sh

quickstart-small:
	$(ENSURE_BASH); bash ./scripts/quickstart.sh --size small

quickstart-medium:
	$(ENSURE_BASH); bash ./scripts/quickstart.sh --size medium

quickstart-large:
	$(ENSURE_BASH); bash ./scripts/quickstart.sh --size large

quickstart-ci:
	$(ENSURE_BASH); bash ./scripts/quickstart.sh --size small --no-server

start:
	$(REQUIRE_VENV); . '$(VENV_ACTIVATE)'; uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

start-prod:
	$(REQUIRE_VENV); . '$(VENV_ACTIVATE)'; uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4

venv:
	$(PYTHON) -m venv .venv

deps:
	$(REQUIRE_VENV); . '$(VENV_ACTIVATE)'; python -m pip install --upgrade pip; pip install -r requirements.txt

db-up:
	docker compose -f $(COMPOSE_FILE) up -d

db-schema:
	if ([string]::IsNullOrEmpty($Env:DB_USER) -or [string]::IsNullOrEmpty($Env:DB_PASSWORD) -or [string]::IsNullOrEmpty($Env:DB_NAME)) { Write-Error "DB_USER, DB_PASSWORD, and DB_NAME must be set."; exit 1 }; mysql -h 127.0.0.1 -u$Env:DB_USER -p$Env:DB_PASSWORD $Env:DB_NAME < sql/schema.sql

seed:
	$seed = [int][math]::Floor((Get-Date -AsUTC).Subtract([datetime]'1970-01-01').TotalSeconds); $(PYTHON) scripts/gen_seed_data.py --seed $seed

seed-reproducible:
	$(PYTHON) scripts/gen_seed_data.py --seed 42

load:
	$(PYTHON) scripts/load_csvs.py

smoke:
	$(PYTHON) scripts/db_smoketest.py

clear-db:
	$(PYTHON) scripts/clear_database.py

clear-seed:
	if (Test-Path '$(SEED_DIR)') { Get-ChildItem '$(SEED_DIR)' -Filter '*.csv' -File | Remove-Item -Force; Write-Output "Seed data cleared" } else { Write-Output "No seed directory found" }

