# Finance Dashboard — Simulated Wealth Manager

A compact Python + MariaDB project that ingests CSV data, simulates multi-user
banking and brokerage activity, and exposes analytics/dashboards for
individuals, companies, and a bank admin view.

---

## Purpose
- Model real-world money flows (checking/savings) and basic brokerage (equities)
  for **multiple user types**: bank admin, corporate, individual.
- Support **internal transfers** (user↔user inside the system) and **external**
  (to/from outside counterparties).
- Track **positions**, **average acquisition cost**, **realized/unrealized P&L**,
  and show useful summaries.

---

## Tech Stack
- **Python** (3.11+) — ETL, business logic, future FastAPI services
- **MariaDB** (via Docker) — persistence
- **SQLAlchemy** — ORM / DB access
- **Pandas** — CSV ingest & transforms
- Optional clients: **DBeaver** (DB GUI)

Logging helpers live in `app/logger.py` and can be reused by scripts as well as
future API processes.

---

## Repository Layout
```
finance/
├─ app/
│  ├─ db/                # SQLAlchemy engine/session helpers
│  ├─ etl/               # classification helpers & future rule sets
│  ├─ services/          # placeholder for domain logic
│  └─ logger.py          # shared logging setup
├─ data/                 # generated seed datasets & streaming output
├─ docker/
│  └─ docker-compose.yaml
├─ old/                  # legacy reference implementation
├─ scripts/
│  ├─ db_smoketest.py    # quick connectivity check (PyMySQL by default)
│  ├─ gen_seed_data.py   # high-volume synthetic dataset generator
│  └─ load_csvs.py       # idempotent CSV loader into MariaDB
├─ sql/
│  └─ schema.sql         # bootstrap DDL (pre-Alembic)
├─ Makefile              # shortcuts for common workflows
├─ README.md
└─ requirements.txt
```

---

## Quick Start

### 1) Prerequisites
- Docker Desktop (or compatible engine)
- Python 3.11+

### 2) Configure environment
Create `.env` from the example (or rely on the baked-in defaults) and adjust as needed:
```ini
# .env example
MARIADB_ROOT_PASSWORD=devroot
MARIADB_DATABASE=finance
MARIADB_USER=app
MARIADB_PASSWORD=apppwd
DB_HOST=127.0.0.1
DB_PORT=3306
DB_NAME=finance
DB_USER=app
DB_PASSWORD=apppwd
DB_DRIVER=mysql+pymysql
SQLALCHEMY_ECHO=false
```

> **Tip:** Compose now reads `.env` automatically when you run `docker compose`,
> so the container starts even if you skip creating the file. Customize the
> values above when you need something other than the defaults.

### 3) Start MariaDB (Docker)
Using Compose (recommended):
```bash
docker compose -f docker/docker-compose.yaml up -d
```
The Compose file maps the database port using `DB_PORT` from your `.env` (default
`3306`), so updating that value keeps local tooling and scripts aligned.

### 4) Create a virtual environment & install deps
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```

### 5) Initialize the database
Pick one:
- Execute `sql/schema.sql` in DBeaver/VS Code SQL editor; or
- Provide Alembic migrations (TODO) and run `alembic upgrade head`.

### 6) Smoke test the connection
```bash
python scripts/db_smoketest.py
```
The script uses PyMySQL by default; override `DB_DRIVER` if you prefer
`mysqlclient`.

### 7) Launch the FastAPI admin dashboard

With the database running and populated, you can start the new web frontend to
explore the administrator landing page:

1. Activate your virtual environment (see step 4) and ensure dependencies are installed.
2. Export any database overrides if you are not using the defaults described in
   step 2 (`DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`,
   `DB_DRIVER`). The FastAPI layer reuses the same settings module as the ETL
   scripts, so the environment only needs to be configured once.
3. From the repository root, run Uvicorn:

   ```bash
   uvicorn app.web.app:app --reload
   ```

   Add `--host 0.0.0.0 --port 8080` (or similar) if you want to change the bind
   address for containers/cloud deployments. In production you can drop
   `--reload` and let a process manager (e.g., gunicorn with `uvicorn.workers`) handle restarts.
4. Open [http://localhost:8000/admin](http://localhost:8000/admin) in your
   browser. The page renders aggregated metrics and recent-account activity by
   calling `AdminDashboardService`, which in turn queries the MariaDB instance.

> **Troubleshooting:** The server logs any database connectivity errors on
> startup. Double-check that the database container is running, credentials
> match, and the schema has been initialized.

---

## Synthetic Data Generation

`scripts/gen_seed_data.py` now produces an expansive, story-driven dataset:

- **Defaults** (`--individuals 2500 --companies 120 --months 18`) yield roughly
  700k transactions, 2.5k users, and 120 corporate accounts—sized to run on an
  M4 MacBook Air (16 GB RAM) while leaving headroom for analytics.
- All values are reproducible with `--seed <int>`.
- Accounts cover checking, savings, and brokerage products so charts can show
  both cash flow and investment performance.

Common commands:
```bash
# Generate the default large dataset under data/seed/
python scripts/gen_seed_data.py

# Lighter run for local debugging
python scripts/gen_seed_data.py --individuals 200 --companies 20 --months 6

# Start an ongoing stream of fresh spend data (writes to data/stream/)
python scripts/gen_seed_data.py --continuous --live-batch-size 150 --live-interval 1.5
```

### Live Streaming
`--continuous` keeps the generator running after the historical snapshot and
appends card-style spend events to `data/stream/transactions_live.csv`. Use this
for real-time dashboard demos—watch charts update while the stream adds new
rows every few seconds.

---

## Narrative Ideas for Dashboards
These scenarios are already embedded in the generator so you can craft charts
or KPIs around them later.

### Individuals
- **Budgeting & cost of living** — monthly salary inflows, rent debits, and
  utilities produce clear fixed-cost vs discretionary patterns.
- **Savings habits** — internal transfers push 8–18% of each salary into savings
  accounts, enabling trend lines for rainy-day funds.
- **Lifestyle segments** — categories such as travel, dining, and subscriptions
  let you cluster users into frugal vs experiential personas.

### Companies
- **Revenue vs. expenses** — recurring customer invoice inflows contrasted with
  vendor, rent, payroll, and quarterly tax outflows provide classic P&L views.
- **Payroll insights** — each employee’s salary hits their employer’s account as
  a paired debit, making headcount cost analyses straightforward.
- **Cash runway** — with 18 months of history, you can highlight seasonality,
  burn rate, and forecast runway for different industries.

### Bank Admin / Portfolio View
- **System-wide spend mix** — aggregate card, SEPA, and internal transfer volumes
  to show how money flows through the institution.
- **Sector performance** — company industries unlock heatmaps (e.g. which sectors
  drive inflows vs. cash burn).
- **Investment trends** — hundreds of investors trade popular tickers (AAPL,
  MSFT, NVDA, TSLA, VWRL, ASML), enabling holdings and P&L dashboards.

Capture these stories in dashboards and docs to help reviewers connect the data
with real-world use cases.

---

## Roadmap Notes
- Flesh out `app/services/` with reusable transaction/trade services before the
  FastAPI layer lands.
- Introduce Alembic migrations that mirror `sql/schema.sql`.
- Wire the streaming feed into background workers or websocket publishers so the
  dashboard front-end can subscribe to live updates.
- Port over any still-useful utilities from `old/` once their modern
  equivalents exist in the new `app/` package.
