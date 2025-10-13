.PHONY: venv deps db-up db-schema seed load smoke

venv:
	python3 -m venv .venv

deps:
	. .venv/bin/activate && pip install -r requirements.txt

db-up:
	docker compose -f docker/docker-compose.yaml up -d

db-schema:
	mysql -h 127.0.0.1 -u$${DB_USER} -p$${DB_PASSWORD} $${DB_NAME} < sql/schema.sql

seed:
	python scripts/gen_seed_data.py

load:
	python scripts/load_csvs.py

smoke:
	python scripts/db_smoketest.py