-- Schema: journal-ledger backbone for checking/brokerage
SET NAMES utf8mb4;
SET time_zone = "+00:00";

CREATE TABLE IF NOT EXISTS user (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(120) NOT NULL,
  email VARCHAR(255) UNIQUE,
  job_title VARCHAR(120) NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS org (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(160) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS membership (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  user_id BIGINT UNSIGNED NOT NULL,
  org_id BIGINT UNSIGNED NOT NULL,
  role VARCHAR(64) NOT NULL DEFAULT 'member',
  is_primary BOOLEAN NOT NULL DEFAULT TRUE,
  start_date DATE NULL,
  end_date DATE NULL,
  UNIQUE KEY uniq_user_org (user_id, org_id),
  INDEX ix_membership_user_active (user_id, is_primary, start_date, end_date),
  INDEX ix_membership_org (org_id),
  FOREIGN KEY (user_id) REFERENCES user(id),
  FOREIGN KEY (org_id)  REFERENCES org(id)
) ENGINE=InnoDB;

-- Safety: attempt to add new columns if schema pre-exists without them
ALTER TABLE membership
  ADD COLUMN IF NOT EXISTS is_primary BOOLEAN NOT NULL DEFAULT TRUE,
  ADD COLUMN IF NOT EXISTS start_date DATE NULL,
  ADD COLUMN IF NOT EXISTS end_date DATE NULL;

CREATE TABLE IF NOT EXISTS currency (
  code CHAR(3) NOT NULL PRIMARY KEY,
  name VARCHAR(64) NOT NULL,
  exponent TINYINT UNSIGNED NOT NULL DEFAULT 2
) ENGINE=InnoDB;

INSERT INTO currency (code, name, exponent) VALUES
  ('EUR', 'Euro', 2),
  ('USD', 'US Dollar', 2),
  ('GBP', 'British Pound Sterling', 2)
ON DUPLICATE KEY UPDATE
  name = VALUES(name),
  exponent = VALUES(exponent);

CREATE TABLE IF NOT EXISTS account_type (
  code VARCHAR(32) NOT NULL PRIMARY KEY,
  description VARCHAR(128) NOT NULL,
  is_cash TINYINT(1) NOT NULL DEFAULT 1,
  is_brokerage TINYINT(1) NOT NULL DEFAULT 0
) ENGINE=InnoDB;

INSERT INTO account_type (code, description, is_cash, is_brokerage) VALUES
  ('checking', 'Checking / current account', 1, 0),
  ('savings', 'Savings deposit account', 1, 0),
  ('brokerage', 'Brokerage / investment account', 0, 1),
  ('operating', 'Corporate operating account', 1, 0)
ON DUPLICATE KEY UPDATE
  description = VALUES(description),
  is_cash = VALUES(is_cash),
  is_brokerage = VALUES(is_brokerage);

CREATE TABLE IF NOT EXISTS account_role (
  code VARCHAR(32) NOT NULL PRIMARY KEY,
  description VARCHAR(128) NOT NULL
) ENGINE=InnoDB;

INSERT INTO account_role (code, description) VALUES
  ('OWNER', 'Primary owner of the account'),
  ('MANAGER', 'May manage the account'),
  ('VIEWER', 'Read-only access'),
  ('EMPLOYEE_CARDHOLDER', 'Issued employee payment card'),
  ('BANK_ADMIN', 'Bank administrator access')
ON DUPLICATE KEY UPDATE
  description = VALUES(description);

CREATE TABLE IF NOT EXISTS txn_channel (
  code VARCHAR(32) NOT NULL PRIMARY KEY,
  description VARCHAR(128) NOT NULL
) ENGINE=InnoDB;

INSERT INTO txn_channel (code, description) VALUES
  ('SEPA', 'SEPA payment'),
  ('CARD', 'Card payment'),
  ('WIRE', 'Wire transfer'),
  ('CASH', 'Cash movement'),
  ('INTERNAL', 'Internal transfer')
ON DUPLICATE KEY UPDATE
  description = VALUES(description);

CREATE TABLE IF NOT EXISTS trade_side (
  code VARCHAR(16) NOT NULL PRIMARY KEY,
  description VARCHAR(128) NOT NULL
) ENGINE=InnoDB;

INSERT INTO trade_side (code, description) VALUES
  ('BUY', 'Purchase of an instrument'),
  ('SELL', 'Sale of an instrument')
ON DUPLICATE KEY UPDATE
  description = VALUES(description);

CREATE TABLE IF NOT EXISTS instrument_type (
  code VARCHAR(32) NOT NULL PRIMARY KEY,
  description VARCHAR(128) NOT NULL
) ENGINE=InnoDB;

INSERT INTO instrument_type (code, description) VALUES
  ('EQUITY', 'Equity security'),
  ('ETF', 'Exchange-traded fund')
ON DUPLICATE KEY UPDATE
  description = VALUES(description);

CREATE TABLE IF NOT EXISTS market (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  mic VARCHAR(10) NOT NULL UNIQUE,
  name VARCHAR(160) NOT NULL,
  timezone VARCHAR(64) NOT NULL,
  country_code CHAR(2) NOT NULL
) ENGINE=InnoDB;

INSERT INTO market (mic, name, timezone, country_code) VALUES
  ('XNAS', 'Nasdaq Stock Market', 'America/New_York', 'US'),
  ('XLON', 'London Stock Exchange', 'Europe/London', 'GB'),
  ('XAMS', 'Euronext Amsterdam', 'Europe/Amsterdam', 'NL')
ON DUPLICATE KEY UPDATE
  name = VALUES(name),
  timezone = VALUES(timezone),
  country_code = VALUES(country_code);

CREATE TABLE IF NOT EXISTS party (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  party_type ENUM('INDIVIDUAL','COMPANY') NOT NULL,
  display_name VARCHAR(160) NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_party_type_name (party_type, display_name)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS individual_profile (
  party_id BIGINT UNSIGNED PRIMARY KEY,
  given_name VARCHAR(80) NOT NULL,
  family_name VARCHAR(80) NOT NULL,
  primary_email VARCHAR(255) NOT NULL UNIQUE,
  residency_country CHAR(2) NULL,
  birth_date DATE NULL,
  FOREIGN KEY (party_id) REFERENCES party(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS company_profile (
  party_id BIGINT UNSIGNED PRIMARY KEY,
  legal_name VARCHAR(160) NOT NULL,
  registration_number VARCHAR(32) UNIQUE,
  tax_identifier VARCHAR(32) UNIQUE,
  industry_code VARCHAR(16) NULL,
  incorporation_date DATE NULL,
  FOREIGN KEY (party_id) REFERENCES party(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS app_role (
  code VARCHAR(32) NOT NULL PRIMARY KEY,
  description VARCHAR(128) NOT NULL
) ENGINE=InnoDB;

INSERT INTO app_role (code, description) VALUES
  ('ADMIN', 'System administrator'),
  ('EMPLOYEE', 'Individual employee access'),
  ('MANAGER', 'Company manager access')
ON DUPLICATE KEY UPDATE
  description = VALUES(description);

CREATE TABLE IF NOT EXISTS app_user (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  party_id BIGINT UNSIGNED NULL,
  username VARCHAR(120) NOT NULL UNIQUE,
  email VARCHAR(255) NOT NULL UNIQUE,
  password_hash VARCHAR(255) NOT NULL,
  is_active TINYINT(1) NOT NULL DEFAULT 1,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_app_user_party (party_id),
  FOREIGN KEY (party_id) REFERENCES party(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS app_user_role (
  app_user_id BIGINT UNSIGNED NOT NULL,
  role_code VARCHAR(32) NOT NULL,
  PRIMARY KEY (app_user_id, role_code),
  FOREIGN KEY (app_user_id) REFERENCES app_user(id),
  FOREIGN KEY (role_code) REFERENCES app_role(code)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS employment_contract (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  employee_party_id BIGINT UNSIGNED NOT NULL,
  employer_party_id BIGINT UNSIGNED NOT NULL,
  position_title VARCHAR(160) NOT NULL,
  start_date DATE NOT NULL,
  end_date DATE NULL,
  is_primary TINYINT(1) NOT NULL DEFAULT 1,
  UNIQUE KEY uq_contract_employee_employer_start (employee_party_id, employer_party_id, start_date),
  INDEX ix_employment_employee (employee_party_id),
  INDEX ix_employment_employer (employer_party_id),
  FOREIGN KEY (employee_party_id) REFERENCES party(id),
  FOREIGN KEY (employer_party_id) REFERENCES party(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS party_relationship (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  from_party_id BIGINT UNSIGNED NOT NULL,
  to_party_id BIGINT UNSIGNED NOT NULL,
  relationship_type VARCHAR(64) NOT NULL,
  start_date DATE NULL,
  end_date DATE NULL,
  UNIQUE KEY uq_party_relationship (from_party_id, to_party_id, relationship_type, start_date),
  INDEX ix_relationship_to_party (to_party_id),
  FOREIGN KEY (from_party_id) REFERENCES party(id),
  FOREIGN KEY (to_party_id) REFERENCES party(id)
) ENGINE=InnoDB;

DROP TABLE IF EXISTS transfer_link;
DROP TABLE IF EXISTS lot;
DROP TABLE IF EXISTS holding;
DROP TABLE IF EXISTS trade;
DROP TABLE IF EXISTS `transaction`;
DROP TABLE IF EXISTS journal_line;
DROP TABLE IF EXISTS journal_entry;
DROP TABLE IF EXISTS counterparty;
DROP TABLE IF EXISTS instrument_identifier;
DROP TABLE IF EXISTS price_daily;
DROP TABLE IF EXISTS price_quote;
DROP TABLE IF EXISTS holding_performance_fact;
DROP TABLE IF EXISTS cash_flow_fact;
DROP TABLE IF EXISTS payroll_fact;
DROP TABLE IF EXISTS user_salary_monthly;
DROP TABLE IF EXISTS instrument;
DROP TABLE IF EXISTS org_party_map;
DROP TABLE IF EXISTS user_party_map;
DROP TABLE IF EXISTS account_party_role;
DROP TABLE IF EXISTS account_membership;
DROP TABLE IF EXISTS account;

CREATE TABLE IF NOT EXISTS account (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  party_id BIGINT UNSIGNED NOT NULL,
  account_type_code VARCHAR(32) NOT NULL,
  currency_code CHAR(3) NOT NULL,
  name VARCHAR(120),
  iban VARCHAR(34) NULL,
  opened_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  closed_at DATETIME NULL,
  INDEX ix_account_party (party_id),
  INDEX ix_account_type (account_type_code),
  UNIQUE KEY uniq_iban (iban),
  FOREIGN KEY (party_id) REFERENCES party(id),
  FOREIGN KEY (account_type_code) REFERENCES account_type(code),
  FOREIGN KEY (currency_code) REFERENCES currency(code)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS account_party_role (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  account_id BIGINT UNSIGNED NOT NULL,
  party_id BIGINT UNSIGNED NOT NULL,
  role_code VARCHAR(32) NOT NULL,
  start_date DATE NULL,
  end_date DATE NULL,
  is_primary TINYINT(1) NOT NULL DEFAULT 0,
  UNIQUE KEY uq_account_party_role (account_id, party_id, role_code),
  INDEX ix_account_party (account_id, party_id),
  FOREIGN KEY (account_id) REFERENCES account(id),
  FOREIGN KEY (party_id) REFERENCES party(id),
  FOREIGN KEY (role_code) REFERENCES account_role(code)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS company_access_grant (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  contract_id BIGINT UNSIGNED NOT NULL,
  app_user_id BIGINT UNSIGNED NOT NULL,
  role_code VARCHAR(32) NOT NULL,
  granted_at DATETIME NOT NULL,
  revoked_at DATETIME NULL,
  UNIQUE KEY uq_company_access (contract_id, app_user_id, role_code),
  INDEX ix_access_contract (contract_id),
  FOREIGN KEY (contract_id) REFERENCES employment_contract(id),
  FOREIGN KEY (app_user_id) REFERENCES app_user(id),
  FOREIGN KEY (role_code) REFERENCES app_role(code)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS user_party_map (
  user_id BIGINT UNSIGNED PRIMARY KEY,
  party_id BIGINT UNSIGNED NOT NULL,
  FOREIGN KEY (user_id) REFERENCES user(id),
  FOREIGN KEY (party_id) REFERENCES party(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS org_party_map (
  org_id BIGINT UNSIGNED PRIMARY KEY,
  party_id BIGINT UNSIGNED NOT NULL,
  FOREIGN KEY (org_id) REFERENCES org(id),
  FOREIGN KEY (party_id) REFERENCES party(id)
) ENGINE=InnoDB;


CREATE TABLE IF NOT EXISTS section (
  id TINYINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(32) NOT NULL UNIQUE
) ENGINE=InnoDB;

INSERT IGNORE INTO section (id, name) VALUES (1,'income'),(2,'expense'),(3,'transfer');

CREATE TABLE IF NOT EXISTS category (
  id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  section_id TINYINT UNSIGNED NOT NULL,
  name VARCHAR(64) NOT NULL,
  UNIQUE KEY uniq_section_name (section_id, name),
  FOREIGN KEY (section_id) REFERENCES section(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS journal_entry (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  entry_code VARCHAR(64) NOT NULL UNIQUE,
  txn_date DATE NOT NULL,
  posted_at DATETIME NOT NULL,
  description VARCHAR(255),
  channel_code VARCHAR(32) NULL,
  counterparty_party_id BIGINT UNSIGNED NULL,
  transfer_reference VARCHAR(64) NULL,
  external_reference VARCHAR(64) NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX ix_journal_transfer (transfer_reference),
  FOREIGN KEY (channel_code) REFERENCES txn_channel(code),
  FOREIGN KEY (counterparty_party_id) REFERENCES party(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS journal_line (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  entry_id BIGINT UNSIGNED NOT NULL,
  account_id BIGINT UNSIGNED NOT NULL,
  party_id BIGINT UNSIGNED NULL,
  amount DECIMAL(18,4) NOT NULL,
  currency_code CHAR(3) NOT NULL,
  category_id INT UNSIGNED NULL,
  line_memo VARCHAR(255),
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX ix_journal_line_entry (entry_id),
  INDEX ix_journal_line_account (account_id),
  INDEX ix_journal_line_party (party_id),
  INDEX ix_journal_line_category (category_id),
  FOREIGN KEY (entry_id) REFERENCES journal_entry(id),
  FOREIGN KEY (account_id) REFERENCES account(id),
  FOREIGN KEY (party_id) REFERENCES party(id),
  FOREIGN KEY (currency_code) REFERENCES currency(code),
  FOREIGN KEY (category_id) REFERENCES category(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS instrument (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  instrument_type_code VARCHAR(32) NOT NULL,
  symbol VARCHAR(32) NOT NULL,
  name VARCHAR(160) NOT NULL,
  primary_currency_code CHAR(3) NOT NULL,
  primary_market_id BIGINT UNSIGNED NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_symbol_market (symbol, primary_market_id),
  FOREIGN KEY (instrument_type_code) REFERENCES instrument_type(code),
  FOREIGN KEY (primary_currency_code) REFERENCES currency(code),
  FOREIGN KEY (primary_market_id) REFERENCES market(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS instrument_identifier (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  instrument_id BIGINT UNSIGNED NOT NULL,
  identifier_type VARCHAR(32) NOT NULL,
  identifier_value VARCHAR(64) NOT NULL,
  UNIQUE KEY uq_identifier (identifier_type, identifier_value),
  INDEX ix_instrument_identifier (instrument_id),
  FOREIGN KEY (instrument_id) REFERENCES instrument(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS price_quote (
  instrument_id BIGINT UNSIGNED NOT NULL,
  price_date DATE NOT NULL,
  quote_type VARCHAR(16) NOT NULL DEFAULT 'CLOSE',
  quote_value DECIMAL(18,6) NOT NULL,
  PRIMARY KEY (instrument_id, price_date, quote_type),
  FOREIGN KEY (instrument_id) REFERENCES instrument(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS fx_rate_daily (
  base CHAR(3) NOT NULL,
  quote CHAR(3) NOT NULL,
  rate_date DATE NOT NULL,
  rate DECIMAL(18,10) NOT NULL,
  PRIMARY KEY (base, quote, rate_date)
) ENGINE=InnoDB;


CREATE TABLE IF NOT EXISTS trade (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  account_id BIGINT UNSIGNED NOT NULL,
  instrument_id BIGINT UNSIGNED NOT NULL,
  trade_time DATETIME NOT NULL,
  side ENUM('BUY','SELL') NOT NULL,
  qty DECIMAL(18,6) NOT NULL,
  price DECIMAL(18,6) NOT NULL,
  fees DECIMAL(18,6) NOT NULL DEFAULT 0,
  tax DECIMAL(18,6) NOT NULL DEFAULT 0,
  currency CHAR(3) NOT NULL DEFAULT 'EUR',
  settle_dt DATE NULL,
  INDEX ix_account_time (account_id, trade_time),
  FOREIGN KEY (account_id) REFERENCES account(id),
  FOREIGN KEY (instrument_id) REFERENCES instrument(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS holding (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  account_id BIGINT UNSIGNED NOT NULL,
  instrument_id BIGINT UNSIGNED NOT NULL,
  qty DECIMAL(18,6) NOT NULL DEFAULT 0,
  avg_cost DECIMAL(18,6) NOT NULL DEFAULT 0,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uniq_pos (account_id, instrument_id),
  FOREIGN KEY (account_id) REFERENCES account(id),
  FOREIGN KEY (instrument_id) REFERENCES instrument(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS lot (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  holding_id BIGINT UNSIGNED NOT NULL,
  trade_id BIGINT UNSIGNED NOT NULL,
  qty DECIMAL(18,6) NOT NULL,
  cost_basis DECIMAL(18,6) NOT NULL,
  FOREIGN KEY (holding_id) REFERENCES holding(id),
  FOREIGN KEY (trade_id)   REFERENCES trade(id)
) ENGINE=InnoDB;

CREATE OR REPLACE VIEW position_agg AS
SELECT h.account_id,
       h.instrument_id,
       h.qty,
       h.avg_cost,
       pq.quote_value AS last_price,
       (h.qty * (pq.quote_value - h.avg_cost)) AS unrealized_pl
FROM holding h
LEFT JOIN price_quote pq
  ON pq.instrument_id = h.instrument_id
 AND pq.quote_type = 'CLOSE'
 AND pq.price_date = (
      SELECT MAX(p2.price_date)
      FROM price_quote p2
      WHERE p2.instrument_id = h.instrument_id
        AND p2.quote_type = 'CLOSE'
    );

CREATE OR REPLACE VIEW v_account_balance AS
SELECT jl.account_id,
       SUM(jl.amount) AS balance
FROM journal_line jl
GROUP BY jl.account_id;

-- Precomputed monthly salary per user and employer
CREATE TABLE IF NOT EXISTS reporting_period (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  period_start DATE NOT NULL,
  period_end DATE NOT NULL,
  label VARCHAR(64) NOT NULL,
  UNIQUE KEY uq_reporting_period (period_start, period_end),
  UNIQUE KEY uq_reporting_label (label)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS payroll_fact (
  reporting_period_id BIGINT UNSIGNED NOT NULL,
  contract_id BIGINT UNSIGNED NOT NULL,
  gross_amount DECIMAL(18,4) NOT NULL,
  net_amount DECIMAL(18,4) NOT NULL,
  taxes_withheld DECIMAL(18,4) NOT NULL,
  PRIMARY KEY (reporting_period_id, contract_id),
  FOREIGN KEY (reporting_period_id) REFERENCES reporting_period(id),
  FOREIGN KEY (contract_id) REFERENCES employment_contract(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS cash_flow_fact (
  reporting_period_id BIGINT UNSIGNED NOT NULL,
  party_id BIGINT UNSIGNED NOT NULL,
  section_id TINYINT UNSIGNED NOT NULL,
  inflow_amount DECIMAL(18,4) NOT NULL DEFAULT 0,
  outflow_amount DECIMAL(18,4) NOT NULL DEFAULT 0,
  net_amount DECIMAL(18,4) NOT NULL DEFAULT 0,
  PRIMARY KEY (reporting_period_id, party_id, section_id),
  INDEX ix_cash_flow_party (party_id),
  FOREIGN KEY (reporting_period_id) REFERENCES reporting_period(id),
  FOREIGN KEY (party_id) REFERENCES party(id),
  FOREIGN KEY (section_id) REFERENCES section(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS holding_performance_fact (
  reporting_period_id BIGINT UNSIGNED NOT NULL,
  party_id BIGINT UNSIGNED NOT NULL,
  instrument_id BIGINT UNSIGNED NOT NULL,
  quantity DECIMAL(18,6) NOT NULL DEFAULT 0,
  cost_basis DECIMAL(18,4) NOT NULL DEFAULT 0,
  market_value DECIMAL(18,4) NOT NULL DEFAULT 0,
  unrealized_pl DECIMAL(18,4) NOT NULL DEFAULT 0,
  PRIMARY KEY (reporting_period_id, party_id, instrument_id),
  INDEX ix_holding_fact_party (party_id),
  FOREIGN KEY (reporting_period_id) REFERENCES reporting_period(id),
  FOREIGN KEY (party_id) REFERENCES party(id),
  FOREIGN KEY (instrument_id) REFERENCES instrument(id)
) ENGINE=InnoDB;
