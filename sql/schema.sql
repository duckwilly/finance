-- Schema: minimal backbone for checking/brokerage
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

CREATE TABLE IF NOT EXISTS account (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  owner_type ENUM('user','org') NOT NULL,
  owner_id BIGINT UNSIGNED NOT NULL,
  type ENUM('checking','savings','brokerage','operating') NOT NULL,
  currency CHAR(3) NOT NULL DEFAULT 'EUR',
  name VARCHAR(120),
  iban VARCHAR(34) NULL,
  opened_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  closed_at DATETIME NULL,
  INDEX ix_owner (owner_type, owner_id),
  INDEX ix_account_type (type),
  UNIQUE KEY uniq_iban (iban)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS account_membership (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  party_type ENUM('user','org') NOT NULL,
  party_id BIGINT UNSIGNED NOT NULL,
  account_id BIGINT UNSIGNED NOT NULL,
  role ENUM('OWNER','MANAGER','VIEWER','EMPLOYEE_CARDHOLDER','BANK_ADMIN') NOT NULL,
  UNIQUE KEY uniq_party_account (party_type, party_id, account_id),
  INDEX ix_account (account_id),
  FOREIGN KEY (account_id) REFERENCES account(id)
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

CREATE TABLE IF NOT EXISTS counterparty (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(160) NOT NULL,
  account_ref VARCHAR(64) NULL,
  bic VARCHAR(11) NULL,
  country_code CHAR(2) NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS instrument (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  symbol VARCHAR(32) NOT NULL,
  name VARCHAR(160) NOT NULL,
  type ENUM('EQUITY','ETF') NOT NULL DEFAULT 'EQUITY',
  isin VARCHAR(16) UNIQUE,
  mic VARCHAR(10) NULL,
  currency CHAR(3) NOT NULL DEFAULT 'EUR',
  UNIQUE KEY uniq_symbol_mic (symbol, mic)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS price_daily (
  instrument_id BIGINT UNSIGNED NOT NULL,
  price_date DATE NOT NULL,
  close_price DECIMAL(18,6) NOT NULL,
  currency CHAR(3) NOT NULL,
  PRIMARY KEY (instrument_id, price_date),
  FOREIGN KEY (instrument_id) REFERENCES instrument(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS fx_rate_daily (
  base CHAR(3) NOT NULL,
  quote CHAR(3) NOT NULL,
  rate_date DATE NOT NULL,
  rate DECIMAL(18,10) NOT NULL,
  PRIMARY KEY (base, quote, rate_date)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `transaction` (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  account_id BIGINT UNSIGNED NOT NULL,
  posted_at DATETIME NOT NULL,
  txn_date DATE NOT NULL,
  amount DECIMAL(18,4) NOT NULL,
  currency CHAR(3) NOT NULL DEFAULT 'EUR',
  direction ENUM('DEBIT','CREDIT') NOT NULL,
  section_id TINYINT UNSIGNED NOT NULL,
  category_id INT UNSIGNED NULL,
  channel ENUM('SEPA','CARD','WIRE','CASH','INTERNAL') NOT NULL DEFAULT 'SEPA',
  description VARCHAR(255),
  counterparty_id BIGINT UNSIGNED NULL,
  transfer_group_id VARCHAR(64) NULL,
  ext_reference VARCHAR(64) NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CHECK (amount > 0),
  INDEX ix_account_posted (account_id, posted_at),
  INDEX ix_transfer (transfer_group_id),
  INDEX ix_counterparty (counterparty_id),
  FOREIGN KEY (account_id) REFERENCES account(id),
  FOREIGN KEY (section_id) REFERENCES section(id),
  FOREIGN KEY (category_id) REFERENCES category(id),
  FOREIGN KEY (counterparty_id) REFERENCES counterparty(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS transfer_link (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  debit_txn_id BIGINT UNSIGNED NOT NULL,
  credit_txn_id BIGINT UNSIGNED NOT NULL,
  UNIQUE KEY uniq_pair (debit_txn_id, credit_txn_id),
  FOREIGN KEY (debit_txn_id) REFERENCES `transaction`(id),
  FOREIGN KEY (credit_txn_id) REFERENCES `transaction`(id)
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
       p.close_price AS last_price,
       (h.qty * (p.close_price - h.avg_cost)) AS unrealized_pl
FROM holding h
LEFT JOIN price_daily p
  ON p.instrument_id = h.instrument_id
 AND p.price_date = (
      SELECT MAX(p2.price_date)
      FROM price_daily p2
      WHERE p2.instrument_id = h.instrument_id
    );

CREATE OR REPLACE VIEW v_account_balance AS
SELECT t.account_id,
       SUM(CASE WHEN t.direction='CREDIT' THEN t.amount ELSE -t.amount END) AS balance
FROM `transaction` t
GROUP BY t.account_id;

-- Precomputed monthly salary per user and employer
CREATE TABLE IF NOT EXISTS user_salary_monthly (
  user_id BIGINT UNSIGNED NOT NULL,
  employer_org_id BIGINT UNSIGNED NOT NULL,
  year SMALLINT UNSIGNED NOT NULL,
  month TINYINT UNSIGNED NOT NULL,
  salary_amount DECIMAL(18,4) NOT NULL,
  PRIMARY KEY (user_id, year, month),
  INDEX ix_salary_employer_ym (employer_org_id, year, month),
  FOREIGN KEY (user_id) REFERENCES user(id),
  FOREIGN KEY (employer_org_id) REFERENCES org(id)
) ENGINE=InnoDB;