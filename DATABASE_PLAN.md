# Database Normalization Plan (3NF + BCNF)

## Goals & Scope
- Bring the financial dashboard domain model to Third Normal Form and Boyce–Codd Normal Form while preserving business requirements (company dashboards, individual dashboards, stock analytics, admin workflows).
- Remove polymorphic foreign keys and redundant attributes that complicate integrity checks, query performance, and seed data generation.
- Provide a migration path from the current `sql/schema.sql` backbone to a maintainable structure that supports reporting windows, holdings analytics, and payroll insights.
- Enforce the new access rule: only individual users authenticate, and their company access is derived from employment relationships instead of company logins.
- Keep iteration simple by evolving `sql/schema.sql` and the seed pipeline together, deferring dedicated migration tooling until a persistent database is required.

## Current Baseline (`sql/schema.sql`)
- Identity split between `user` (individuals) and `org` (companies) with `membership` to connect them.
- Accounts owned by either `user` or `org` via `account.owner_type`/`owner_id` and additional participation tracked in `account_membership`.
- Transactions recorded as single rows in ``transaction`` with both `section_id` and `category_id`, plus per-transaction metadata (`direction`, `channel`, `transfer_group_id`).
- Market data captured through `instrument`, `price_daily`, `fx_rate_daily`, and position tables (`holding`, `lot`, `trade`), with summary views for balances and unrealised P/L.
- Payroll summary stored in `user_salary_monthly`.

## Normalization Gaps Detected
- **Polymorphic ownership (`account.owner_type`, `account_membership.party_type`)** – violates BCNF because `owner_id` determines the referenced table. Integrity relies on application code instead of referential constraints.
- **Redundant category hierarchy** – `transaction` keeps both `section_id` and `category_id`, but `category.section_id` already identifies a section. This creates update anomalies and breaches 3NF (partial dependency on non-key attribute).
- **Employment vs. membership** – `membership` mixes employment contract data (`start_date`, `end_date`, `is_primary`) with generic organisation links. Functional dependencies depend on both party identities and role semantics, leaving the table outside BCNF.
- **Market instrument attributes** – `instrument` stores `isin`, `mic`, `currency`, but `price_daily.currency` depends solely on `instrument_id`. Keeping the column causes redundancy.
- **Payroll summary table** – `user_salary_monthly` uses `(user_id, year, month)` as the primary key but also stores `employer_org_id`. That attribute depends on the contract, not the natural key, leading to multi-valued dependency issues when a user has multiple employers in a month.
- **Seed data coupling** – `scripts/gen_seed_data.py` writes CSVs tailored to the current layout (e.g., owner type enums, membership). Any changes must account for this tight coupling.

## Proposed 3NF + BCNF Structure

### 1. Party & Identity Layer
- `party (party_id PK, party_type, display_name, created_at)`  
  Functional dependency: `party_id → {party_type, display_name, created_at}` satisfies BCNF.
- `individual_profile (party_id PK/FK → party, given_name, family_name, birth_date, primary_email UNIQUE, residency_country)`  
  Individual-specific attributes move out of the generic `user` table.
- `company_profile (party_id PK/FK → party, legal_name, registration_number, tax_identifier, industry_code, incorporation_date)`  
  Replaces `org` while normalising identifiers and classification.
- `app_user (app_user_id PK, party_id FK → party NULLABLE, username UNIQUE, password_hash, is_active, created_at)`  
  Handles authentication separately from the party concept. Only `party_type = 'INDIVIDUAL'` may be linked, guaranteeing that companies themselves never authenticate while still allowing admin-only users (NULL party).
- `app_user_role (app_user_id FK, role_code FK → app_role, PRIMARY KEY (app_user_id, role_code))` with `app_role (role_code PK, description)`  
  Provides RBAC without storing comma-separated roles.

### 2. Party Relationships
- `employment_contract (contract_id PK, employee_party_id FK → party, employer_party_id FK → party, position_title, start_date, end_date, is_primary BOOLEAN)`  
  Replaces `membership`. Keys: `(employee_party_id, employer_party_id, start_date)` prevents overlapping duplicates and meets BCNF because all non-key attributes depend on the full key. Active contracts are the source of truth for linking authenticated employees to company workspaces.
- `party_relationship (relationship_id PK, from_party_id, to_party_id, relationship_type, start_date, end_date)`  
  Generic hook for board memberships, accountants, guardians, etc., without overloading employment.
- `company_access_grant (contract_id FK → employment_contract, app_user_id FK → app_user, access_role_code FK → app_role, granted_at, revoked_at, PRIMARY KEY (contract_id, app_user_id, access_role_code))`  
  Materialises which authenticated employees can act on behalf of a company. The employer is derived through the contract, keeping the relation in BCNF while preventing access grants for contractors without employment history.

### 3. Accounts & Ownership
- `account (account_id PK, account_number UNIQUE, iban UNIQUE NULL, account_type_code FK → account_type, currency_code FK → currency, name, opened_at, closed_at)`  
  Removes polymorphic ownership from the base table.
- `account_type (account_type_code PK, description, is_cash BOOLEAN, is_brokerage BOOLEAN)`  
  Normalises the current enum.
- `account_party_role (account_id FK, party_id FK, role_code FK → account_role, start_date, end_date, is_primary)` with PK `(account_id, party_id, role_code)`  
  Replaces both `account.owner_type` and `account_membership`. BCNF satisfied because composite key determines all attributes. Company-controlled accounts carry a `role_code = 'OWNER'` row for the company party and optional delegated rows tying back to employees through `company_access_grant`.
- `account_role (role_code PK, description)` enumerates OWNER/MANAGER/VIEWER/etc.

### 4. Ledger & Cash Movements
- `journal_entry (entry_id PK, entry_code UNIQUE, txn_date, posted_at, description, channel_code FK → txn_channel, counterparty_party_id FK → party NULL, transfer_reference, external_reference, created_at)`  
  Captures document-level metadata.
- `journal_line (line_id PK, entry_id FK → journal_entry, account_id FK → account, amount DECIMAL SIGNED, currency_code FK → currency, category_id FK → txn_category NULL, related_party_id FK → party NULL, line_memo)`  
  Double-entry ledger: enforce `SUM(amount) = 0` per entry via database constraint. Eliminates `direction`.
- `txn_channel (channel_code PK, description)` replaces the enum in `transaction`.
- `txn_section (section_code PK, name)` and `txn_category (category_id PK, section_code FK → txn_section, category_code UNIQUE, name)`  
  `journal_line` references only `txn_category`; section is derived, restoring 3NF.
- `transfer_link` becomes redundant because balanced journal entries inherently link both sides; retain only if external bank sync requires mapping.

### 5. Market Data & Holdings
- `instrument (instrument_id PK, instrument_type_code FK → instrument_type, display_symbol, name, primary_currency_code FK → currency, primary_market_id FK → market)`  
  Drops redundant `currency` columns elsewhere.
- `instrument_type (instrument_type_code PK, description)` standardises the enum (EQUITY/ETF/etc).
- `market (market_id PK, mic UNIQUE, name, timezone, country_code)`  
  Moves `mic` out of `instrument`.
- `instrument_identifier (identifier_id PK, instrument_id FK, identifier_type, identifier_value UNIQUE)`  
  Holds ISIN, ticker, CUSIP, etc., without null columns.
- `price_quote (instrument_id FK, price_date, quote_type, value, currency_code, PRIMARY KEY (instrument_id, price_date, quote_type))`  
  Generalises `price_daily` for OHLC or volume if needed. In BCNF because `(instrument_id, price_date, quote_type)` is the key.
- `fx_rate (currency_pair_id FK → currency_pair, rate_date, rate_value, PRIMARY KEY (currency_pair_id, rate_date))` with `currency_pair (currency_pair_id PK, base_currency_code, quote_currency_code UNIQUE)`  
  Normalises composite key storage.
- `trade (trade_id PK, account_id FK, instrument_id FK, trade_time, settlement_date, side_code FK → trade_side, quantity, price, fees_amount, tax_amount, trade_currency_code)`  
  Keep `trade_currency_code` to allow non-native settlements. `trade_side` table replaces enum.
- `lot (lot_id PK, account_id FK, instrument_id FK, open_trade_id FK → trade, close_trade_id FK → trade NULL, quantity_opened, quantity_closed, cost_basis)`  
  Distinguishes open/close legs while keeping dependencies on the full key.
- `position_snapshot (snapshot_id PK, account_id, instrument_id, as_of_date, quantity, average_cost, PRIMARY KEY (account_id, instrument_id, as_of_date))`  
  Derived nightly to simplify dashboard joins; still BCNF because key determines all values.

### 6. Reporting & Analytics
- `reporting_period (period_id PK, period_start_date, period_end_date, label)`  
  Canonical periods for net worth and cash-flow analytics.
- `cash_flow_fact (fact_id PK, period_id FK, party_id FK, account_id FK NULL, category_id FK NULL, inflow_amount, outflow_amount)`  
  Pre-aggregated measures for dashboards; meets 3NF by using surrogate key plus FKs.
- `payroll_fact (fact_id PK, contract_id FK → employment_contract, period_id FK → reporting_period, gross_amount, net_amount, taxes_withheld)`  
  Supersedes `user_salary_monthly`.
- `holding_performance_fact (fact_id PK, period_id FK, account_id, instrument_id, unrealized_pl, realized_pl, dividends_received)`  
  Feeds stock leaderboards.

### 7. Reference Data
- `currency (currency_code PK, name, exponent)` centralises ISO currencies.
- `trade_side`, `txn_channel`, `account_type`, `account_role`, `instrument_type` as lookup tables for enums.
- Optional `country`, `industry`, `tax_regime` tables if compliance reporting is required.

## Old vs. New Mapping
| Current table | Limitation | Proposed replacement | Benefit |
| --- | --- | --- | --- |
| `user` | Mixes individual data with login state | `party` + `individual_profile` + `app_user` | Clear separation of identity vs. authentication |
| `org` | Lacks corporate metadata | `party` + `company_profile` | Supports compliance and reporting |
| `membership` | Combines employment and general relationships | `employment_contract` + `company_access_grant` + `party_relationship` | Enforces contract semantics; derives company access from employee logins |
| `account` + `account_membership` | Polymorphic ownership | `account` + `account_party_role` | True referential constraints; multi-owner accounts |
| ``transaction`` | Single-row ledger with redundant section | `journal_entry` + `journal_line` | Double-entry accounting, 3NF category handling |
| `section`/`category` | Partial dependency in `transaction` | `txn_section`/`txn_category` | Derived section, no duplicates |
| `price_daily` | Currency redundant | `price_quote` (no redundant currency) | Cleaner market data history |
| `user_salary_monthly` | Cannot support multiple employers | `payroll_fact` tied to `employment_contract` | Accurate payroll analytics |

## Implementation Roadmap (Agent Checklist)
1. **Establish reference lookups**
   - Update `sql/schema.sql` to define `currency`, `account_type`, `account_role`, `txn_channel`, `trade_side`, `instrument_type`, and `market`.
   - Extend `scripts/gen_seed_data.py` to emit CSVs for these tables before other loads.
2. **Introduce party system**
   - Create tables `party`, `individual_profile`, `company_profile`, `app_user`, `app_role`, `app_user_role`.
   - Backfill parties from `user`/`org`; link individuals to `app_user` records and assign roles.
3. **Model employment access**
   - Add `employment_contract`, `party_relationship`, `company_access_grant`.
   - Migrate `membership` data into contracts; derive access grants for active employees.
4. **Refactor account ownership**
   - Create `account_party_role`; migrate `account.owner_type` and `account_membership` rows into the new table.
   - Update services to compute visibility via company access + account roles.
5. **Convert ledger to journal**
   - Introduce `journal_entry`, `journal_line` in `sql/schema.sql`; adapt seed generation to emit balanced entries.
   - Remove `direction`, `section_id`, and enum columns once the rebuilt database validates.
   - ✅ Legacy `transaction`/`transfer_link` tables and the `counterparty` entity removed; seeds and loaders now rely solely on journals.
6. **Normalise market data**
   - Split `instrument` into `instrument`, `instrument_identifier`, link to `market` and `currency`.
   - Replace `price_daily` with `price_quote`; adjust holdings loaders accordingly.
7. **Payroll & analytics**
   - Add `reporting_period`, `cash_flow_fact`, `payroll_fact`, `holding_performance_fact`.
   - Rewire dashboards and services to use the fact tables.
8. **Seed pipeline overhaul**
   - Update `scripts/gen_seed_data.py` to emit parties, app users, contracts, access grants, account-party roles, journals, and lookup CSVs.
   - ✅ Legacy `transactions.csv` output removed; journal CSVs validated for zero-balance before writing.
9. **Application updates**
   - Refactor models/schemas/services to consume new tables; remove direct `user`/`org` joins.
     - ✅ ORM classes now expose party/app user constructs; services resolve dashboard visibility through party IDs and access grants.
     - ✅ Individual and company dashboards now aggregate balances and category breakdowns from `journal_entry`/`journal_line` plus fact tables—no direct `transaction` dependency remains.
     - ✅ Admin tooling now pulls metrics, listings, and transaction explorers from the journal/fact model; the legacy `transaction` table is no longer consulted by services.
   - Implement access checks that flow through `company_access_grant` and `account_party_role`.
     - ✅ Security provider authenticates against `app_user`, issuing access data derived from employment + grants.
10. **Cleanup**
    - Delete legacy table definitions from `sql/schema.sql` once services and seeds rely solely on the new schema.
      - ✅ `transaction`, `transfer_link`, and `counterparty` tables dropped from the schema; compatibility paths now operate through the journal.
    - Retire any temporary compatibility views after the codebase fully adopts the new model.
      - ✅ Placeholder schemas removed; service layer consumes typed responses.
      - ✅ Legacy `Membership` model removed; employment contracts and access grants now cover all lookups.

## BCNF/3NF Compliance Notes
- Every table either has a single-attribute primary key or uses a fully dependent composite key; no non-key attribute depends on part of a composite.
- Lookup codes replace enums so foreign-key references enforce valid values.
- Derived attributes (section name, party display name, current balance) are intentionally materialised via views or facts instead of stored alongside base facts, preventing update anomalies.
- Double-entry ledger ensures financial integrity, and constraints over `(entry_id)` enforce BCNF by tying all metadata to the entry rather than duplicating it per line.

## Migration & Seed Data Considerations
- Update `scripts/gen_seed_data.py` to output CSVs for the new tables (parties, profiles, app users, employment contracts, company access grants, account-party roles, journal entries/lines, lookup tables). The script can still orchestrate totals but should respect surrogate IDs generated after inserts.
- When composing account ownership seeds, derive the company `account_party_role` rows from the new company parties and add delegated roles only for authenticated employees covered by `company_access_grant`.
- Emit balanced debit and credit `journal_line` rows instead of single-sided `transaction` records; infer section from the linked `txn_category` to avoid storing redundant data.
- Materialise reference data CSVs (`account_type`, `account_role`, `txn_channel`, `trade_side`, `instrument_type`, `currency`, `market`) so enums disappear from the schema. Ensure the generator populates them idempotently before dependent facts.
- Stage schema changes carefully: extend `sql/schema.sql`, rebuild the database via `make quickstart`, then drop deprecated columns (`owner_type`, `section_id`, etc.) once backfills succeed.
- Provide compatibility views (`legacy_user`, `legacy_transaction`) during transition so existing dashboards keep functioning while services migrate.
- Build seed data for `company_access_grant` by joining active employment contracts with generated app users so company dashboards are accessible only through employees.
- Split instrument metadata during seeding: load `instrument`, `instrument_identifier`, `market`, and nightly `price_quote` rows with currencies referenced through the shared `currency` table.
- Replace the `user_salary_monthly` CSV with a `payroll_fact` export keyed by `employment_contract` + reporting period so multi-employer months stay consistent.
- Regenerate `price_quote` and `fx_rate` seeds using existing CSVs but split currency metadata into reference tables.
- Recalculate holdings facts from `trade` and `lot` during migration to keep stock leaderboards accurate.

## Next Steps
- Validate the plan against real workflows (individual cash flow, company payroll, stock leaderboard) before starting Step 1 of the roadmap.
- Prototype the company-access path (app user → employment contract → company_access_grant → account_party_role) to de-risk Steps 2–4.
- Draft `sql/schema.sql` updates for the reference lookups (Step 1) and party system (Step 2); run smoke tests against regenerated seed data.
- Spike the seed-data pipeline overhaul (Step 8) to confirm CSV formats and FK ordering.
- Capture updated seed-data expectations in `README.md` once the spike results look stable.
