"""Database schema description for the AI chatbot.

This module provides a comprehensive description of the finance dashboard database
schema to help the AI chatbot understand the data structure and generate accurate SQL queries.
"""

DATABASE_SCHEMA = """
# Finance Dashboard Database Schema

## Core Entities

### party
- Represents both individuals and companies
- Columns: id (BIGINT), party_type (ENUM: 'INDIVIDUAL', 'COMPANY'), display_name (VARCHAR), created_at (DATETIME)

### individual_profile
- Detailed information about individual parties
- Columns: party_id (BIGINT FK to party), given_name, family_name, primary_email, date_of_birth, nationality
- Links to party table via party_id

### company_profile
- Detailed information about company parties
- Columns: party_id (BIGINT FK to party), legal_name, registration_number, incorporation_date, country_code
- Links to party table via party_id

### user
- Application users (individuals)
- Columns: id (BIGINT), name (VARCHAR), email (VARCHAR), job_title (VARCHAR), created_at (TIMESTAMP)

### org
- Organizations/companies
- Columns: id (BIGINT), name (VARCHAR), created_at (TIMESTAMP)

### membership
- Links users to organizations
- Columns: id, user_id (FK to user), org_id (FK to org), role (VARCHAR), is_primary (BOOLEAN), start_date, end_date

## Accounts & Transactions

### account
- Financial accounts (checking, savings, brokerage, operating)
- Columns: id (BIGINT), person_id (FK to party), company_id (FK to party), account_type_code (FK), currency_code (FK), balance (DECIMAL 18,2), is_active (BOOLEAN)
- IMPORTANT: Either person_id OR company_id is set (not both)

### account_type
- Types of accounts: checking, savings, brokerage, operating
- Columns: code (VARCHAR PK), description, is_cash (BOOLEAN), is_brokerage (BOOLEAN)

### section
- High-level categorization (income/expense)
- Columns: id (BIGINT), name (VARCHAR)
- Values: 'income', 'expense'

### category
- Detailed categories for journal entries
- Columns: id (BIGINT), section_id (FK to section), name (VARCHAR)
- Examples: Groceries, Transport, Salary, Investment Income
- Links to section to determine if it's income or expense

IMPORTANT: There is NO 'transaction' or 'transactions' table!
All financial data is in the double-entry journal system (journal_entry and journal_line tables)

## Journal & Ledger (Double-Entry Accounting)

### reporting_period
- Fiscal periods for reporting
- Columns: id (BIGINT), period_start (DATE), period_end (DATE), is_closed (BOOLEAN)

### journal_entry
- Double-entry journal entries
- Columns: id (BIGINT), entry_code (VARCHAR), txn_date (DATE), posted_at (DATETIME), description (VARCHAR), channel_code (VARCHAR)
- IMPORTANT: Use txn_date for date filtering (NOT entry_date)

### journal_line
- Individual lines within journal entries (must balance to zero per entry)
- Columns: id (BIGINT), entry_id (FK to journal_entry), account_id (FK to account),
  debit_amount (DECIMAL 18,2), credit_amount (DECIMAL 18,2), description (TEXT)
- Each entry has multiple lines that sum to zero (debits = credits)

## Investments & Trading

### instrument
- Financial instruments (stocks, ETFs)
- Columns: id (BIGINT), ticker (VARCHAR), instrument_type_code (FK), currency_code (FK),
  company_name (VARCHAR), market_id (FK to market)

### instrument_price
- Historical prices for instruments
- Columns: id (BIGINT), instrument_id (FK), price_date (DATE), close_price (DECIMAL 18,6)

### trade
- Stock/ETF trades
- Columns: id (BIGINT), account_id (FK), instrument_id (FK), trade_date (DATE),
  settlement_date (DATE), side (ENUM: 'BUY', 'SELL'), quantity (DECIMAL 18,6),
  price_per_share (DECIMAL 18,6), commission (DECIMAL 18,2)

### holding
- Current investment positions
- Columns: id (BIGINT), account_id (FK), instrument_id (FK), quantity (DECIMAL 18,6),
  weighted_average_cost (DECIMAL 18,6), last_updated (DATETIME)

## Employment & Access

### employment_contract
- Employment relationships between individuals and companies
- Columns: id (BIGINT), employee_party_id (FK to party), employer_party_id (FK to party),
  position_title (VARCHAR), start_date (DATE), end_date (DATE), is_primary (BOOLEAN)

### company_access_grant
- Grants individuals access to company accounts
- Columns: id (BIGINT), company_party_id (FK), individual_party_id (FK),
  account_id (FK), role_code (FK to account_role)

## Authentication

### app_user
- Application users with authentication
- Columns: id (BIGINT), party_id (FK to party), username (VARCHAR UNIQUE),
  email (VARCHAR UNIQUE), password_hash (VARCHAR), is_active (BOOLEAN)

### app_role
- Available roles: ADMIN, EMPLOYEE, COMPANY_ADMIN, VIEWER
- Columns: code (VARCHAR PK), description (VARCHAR)

### app_user_role
- Links users to roles
- Columns: app_user_id (FK), role_code (FK)

## Fact Tables (Pre-aggregated)

### payroll_fact
- Monthly payroll summary data
- Columns: period_id (FK), employee_party_id (FK), employer_party_id (FK),
  gross_pay (DECIMAL), net_pay (DECIMAL), tax (DECIMAL)

### cash_flow_fact
- Monthly cash flow summaries by category
- Columns: period_id (FK), account_id (FK), category_id (FK),
  total_amount (DECIMAL), transaction_count (INT)

### holding_performance_fact
- Investment performance metrics
- Columns: period_id (FK), account_id (FK), instrument_id (FK),
  avg_quantity (DECIMAL), market_value (DECIMAL), unrealized_gain_loss (DECIMAL)

## Reference Data

### currency
- Supported currencies: EUR, USD, GBP
- Columns: code (CHAR 3 PK), name (VARCHAR), exponent (INT)

### market
- Stock exchanges
- Columns: id (BIGINT), mic (VARCHAR UNIQUE), name (VARCHAR), timezone (VARCHAR), country_code (CHAR 2)

### fx_rate
- Currency exchange rates
- Columns: id (BIGINT), from_currency (FK), to_currency (FK), rate_date (DATE), rate (DECIMAL 18,6)

## Important Notes for SQL Generation

1. **Database Type**: MariaDB/MySQL
   - Use MySQL date functions: CURDATE(), DATE_SUB(CURDATE(), INTERVAL 1 MONTH)
   - NOT SQLite syntax: Do NOT use date('now') or date('now', '-1 month')
   - Use YEAR(), MONTH(), DATE_FORMAT() for date manipulation

2. **CRITICAL - Transaction Data Location**:
   - There is NO 'transaction' or 'transactions' table!
   - ALL financial transactions are in: journal_line table
   - Join path: journal_line -> journal_entry -> account -> category -> section
   - The journal_line table has columns: id, entry_id, account_id, party_id, amount, currency_code, category_id

3. **Account Ownership**: Accounts have EITHER person_id OR company_id (not both)
   - For individual queries: WHERE account.person_id = {person_id}
   - For company queries: WHERE account.company_id = {company_id}
   - For admin queries: No filter (can see all)

4. **Income vs Expense Classification**:
   - Use the section table to determine if category is income or expense
   - Join: journal_line -> category -> section
   - WHERE section.name = 'income' for income
   - WHERE section.name = 'expense' for expenses
   - Amounts in journal_line.amount are the raw values

5. **Common Query Patterns**:
   - Expenses by category:
     ```sql
     SELECT c.name as category, SUM(ABS(jl.amount)) as total
     FROM journal_line jl
     JOIN journal_entry je ON jl.entry_id = je.id
     JOIN account a ON jl.account_id = a.id
     LEFT JOIN category c ON jl.category_id = c.id
     LEFT JOIN section s ON c.section_id = s.id
     WHERE s.name = 'expense'
     GROUP BY c.name
     ```

   - Income by month:
     ```sql
     SELECT DATE_FORMAT(je.txn_date, '%Y-%m') as month, SUM(ABS(jl.amount)) as total
     FROM journal_line jl
     JOIN journal_entry je ON jl.entry_id = je.id
     JOIN account a ON jl.account_id = a.id
     LEFT JOIN category c ON jl.category_id = c.id
     LEFT JOIN section s ON c.section_id = s.id
     WHERE s.name = 'income'
     GROUP BY month
     ```

6. **Date Filtering Examples**:
   - Current month: WHERE YEAR(je.txn_date) = YEAR(CURDATE()) AND MONTH(je.txn_date) = MONTH(CURDATE())
   - Last month: WHERE je.txn_date >= DATE_SUB(CURDATE(), INTERVAL 1 MONTH)
   - This year: WHERE YEAR(je.txn_date) = YEAR(CURDATE())
   - IMPORTANT: Use je.txn_date for journal transactions (NOT entry_date)
   - Use trade_date for trades

7. **Currency Handling**:
   - Most amounts in DECIMAL(18,2) format
   - Prices in DECIMAL(18,6) for precision
   - Use fx_rate table for currency conversions
"""
