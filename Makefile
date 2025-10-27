.PHONY: quickstart quickstart-small quickstart-medium quickstart-large quickstart-ci start start-prod venv deps db-up db-schema seed seed-reproducible load smoke clear-db clear-seed

quickstart:
	./scripts/quickstart.sh

quickstart-small:
	./scripts/quickstart.sh --size small

quickstart-medium:
	./scripts/quickstart.sh --size medium

quickstart-large:
	./scripts/quickstart.sh --size large

quickstart-ci:
	./scripts/quickstart.sh --size small --no-server

start:
	@if [ ! -d .venv ]; then echo "Virtual environment not found. Run 'make venv' first."; exit 1; fi
	. .venv/bin/activate && uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

start-prod:
	@if [ ! -d .venv ]; then echo "Virtual environment not found. Run 'make venv' first."; exit 1; fi
	. .venv/bin/activate && uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4

venv:
	python3 -m venv .venv

deps:
	. .venv/bin/activate && pip install -r requirements.txt

db-up:
	docker compose -f docker/docker-compose.yaml up -d

db-schema:
	mysql -h 127.0.0.1 -u$${DB_USER} -p$${DB_PASSWORD} $${DB_NAME} < sql/schema.sql

seed:
	python scripts/gen_seed_data.py --seed $$(date +%s)

seed-reproducible:
	python scripts/gen_seed_data.py --seed 42

load:
	python scripts/load_csvs.py

smoke:
	python scripts/db_smoketest.py

clear-db:
	python scripts/clear_database.py

clear-seed:
	rm -f data/seed/*.csv && echo "Seed data cleared"
