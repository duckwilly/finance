"""ORM models representing corporate entities."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import BigInteger, DateTime, Integer, String, select, func, and_, extract
from sqlalchemy.orm import Mapped, mapped_column, Session

from .base import Base
from .transactions import Account, Transaction, Section, Category, AccountOwnerType, TransactionDirection

_ID_TYPE = BigInteger().with_variant(Integer, "sqlite")


class Company(Base):
    """Represents an organisation/company in the system."""

    __tablename__ = "org"

    id: Mapped[int] = mapped_column(_ID_TYPE, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), nullable=False
    )

    @staticmethod
    def get_employee_count(session: Session, company_id: int) -> int:
        """Count users with salary transactions from this company."""
        salary_query = (
            select(func.count(func.distinct(Account.owner_id)))
            .select_from(Account)
            .join(Transaction, Transaction.account_id == Account.id)
            .join(Section, Section.id == Transaction.section_id)
            .outerjoin(Category, Category.id == Transaction.category_id)
            .where(
                Account.owner_type == AccountOwnerType.USER,
                Section.name == "income",
                Category.name == "Salary",
                Transaction.description.like(f"Salary from {Company.name}%")
            )
        )
        
        # Get company name first
        company_name = session.execute(
            select(Company.name).where(Company.id == company_id)
        ).scalar_one_or_none()
        
        if not company_name:
            return 0
            
        # Count employees with salary from this company
        employee_count = session.execute(
            select(func.count(func.distinct(Account.owner_id)))
            .select_from(Account)
            .join(Transaction, Transaction.account_id == Account.id)
            .join(Section, Section.id == Transaction.section_id)
            .outerjoin(Category, Category.id == Transaction.category_id)
            .where(
                Account.owner_type == AccountOwnerType.USER,
                Section.name == "income",
                Category.name == "Salary",
                Transaction.description.like(f"Salary from {company_name}%")
            )
        ).scalar_one() or 0
        
        return employee_count

    @staticmethod
    def get_monthly_salary_cost(session: Session, company_id: int) -> Decimal:
        """Sum of most recent month's salary payments to employees."""
        # Get company name
        company_name = session.execute(
            select(Company.name).where(Company.id == company_id)
        ).scalar_one_or_none()
        
        if not company_name:
            return Decimal("0")
        
        # Find the most recent full month from employee salary transactions
        latest_month_query = (
            select(
                extract("year", Transaction.txn_date).label("year"),
                extract("month", Transaction.txn_date).label("month")
            )
            .select_from(Account)
            .join(Transaction, Transaction.account_id == Account.id)
            .join(Section, Section.id == Transaction.section_id)
            .outerjoin(Category, Category.id == Transaction.category_id)
            .where(
                Account.owner_type == AccountOwnerType.USER,
                Section.name == "income",
                Category.name == "Salary",
                Transaction.description.like(f"Salary from {company_name}%")
            )
            .order_by(Transaction.txn_date.desc())
            .limit(1)
        )
        
        latest_month = session.execute(latest_month_query).first()
        if not latest_month:
            return Decimal("0")
        
        # Get the most recent salary transaction per employee for that month
        latest_salary_subquery = (
            select(
                Account.owner_id.label("user_id"),
                Transaction.amount.label("salary_amount"),
                func.row_number().over(
                    partition_by=Account.owner_id,
                    order_by=Transaction.txn_date.desc()
                ).label("rn")
            )
            .select_from(Account)
            .join(Transaction, Transaction.account_id == Account.id)
            .join(Section, Section.id == Transaction.section_id)
            .outerjoin(Category, Category.id == Transaction.category_id)
            .where(
                Account.owner_type == AccountOwnerType.USER,
                Section.name == "income",
                Category.name == "Salary",
                Transaction.description.like(f"Salary from {company_name}%"),
                extract("year", Transaction.txn_date) == latest_month.year,
                extract("month", Transaction.txn_date) == latest_month.month
            )
            .subquery()
        )
        
        # Sum the most recent salary transaction per employee
        salary_cost = session.execute(
            select(func.coalesce(func.sum(latest_salary_subquery.c.salary_amount), 0))
            .select_from(latest_salary_subquery)
            .where(latest_salary_subquery.c.rn == 1)
        ).scalar_one() or Decimal("0")
        
        return salary_cost

    @staticmethod
    def get_monthly_income(session: Session, company_id: int) -> Decimal:
        """Sum of income transactions in most recent full month."""
        # Find the most recent full month
        latest_month_query = (
            select(
                extract("year", Transaction.txn_date).label("year"),
                extract("month", Transaction.txn_date).label("month")
            )
            .select_from(Account)
            .join(Transaction, Transaction.account_id == Account.id)
            .where(
                Account.owner_type == AccountOwnerType.ORG,
                Account.owner_id == company_id
            )
            .order_by(Transaction.txn_date.desc())
            .limit(1)
        )
        
        latest_month = session.execute(latest_month_query).first()
        if not latest_month:
            return Decimal("0")
        
        # Sum income for that month
        monthly_income = session.execute(
            select(func.coalesce(func.sum(Transaction.amount), 0))
            .select_from(Account)
            .join(Transaction, Transaction.account_id == Account.id)
            .join(Section, Section.id == Transaction.section_id)
            .where(
                Account.owner_type == AccountOwnerType.ORG,
                Account.owner_id == company_id,
                Section.name == "income",
                Transaction.direction == TransactionDirection.CREDIT,
                extract("year", Transaction.txn_date) == latest_month.year,
                extract("month", Transaction.txn_date) == latest_month.month
            )
        ).scalar_one() or Decimal("0")
        
        return monthly_income

    @staticmethod
    def get_monthly_expenses(session: Session, company_id: int) -> Decimal:
        """Sum of expense transactions in most recent full month."""
        # Find the most recent full month
        latest_month_query = (
            select(
                extract("year", Transaction.txn_date).label("year"),
                extract("month", Transaction.txn_date).label("month")
            )
            .select_from(Account)
            .join(Transaction, Transaction.account_id == Account.id)
            .where(
                Account.owner_type == AccountOwnerType.ORG,
                Account.owner_id == company_id
            )
            .order_by(Transaction.txn_date.desc())
            .limit(1)
        )
        
        latest_month = session.execute(latest_month_query).first()
        if not latest_month:
            return Decimal("0")
        
        # Sum expenses for that month
        monthly_expenses = session.execute(
            select(func.coalesce(func.sum(Transaction.amount), 0))
            .select_from(Account)
            .join(Transaction, Transaction.account_id == Account.id)
            .join(Section, Section.id == Transaction.section_id)
            .where(
                Account.owner_type == AccountOwnerType.ORG,
                Account.owner_id == company_id,
                Section.name == "expense",
                Transaction.direction == TransactionDirection.DEBIT,
                extract("year", Transaction.txn_date) == latest_month.year,
                extract("month", Transaction.txn_date) == latest_month.month
            )
        ).scalar_one() or Decimal("0")
        
        return monthly_expenses

    @staticmethod
    def get_total_profit(session: Session, company_id: int) -> Decimal:
        """Net profit over entire simulated period (income - expenses)."""
        # Sum all income
        total_income = session.execute(
            select(func.coalesce(func.sum(Transaction.amount), 0))
            .select_from(Account)
            .join(Transaction, Transaction.account_id == Account.id)
            .join(Section, Section.id == Transaction.section_id)
            .where(
                Account.owner_type == AccountOwnerType.ORG,
                Account.owner_id == company_id,
                Section.name == "income",
                Transaction.direction == TransactionDirection.CREDIT
            )
        ).scalar_one() or Decimal("0")
        
        # Sum all expenses
        total_expenses = session.execute(
            select(func.coalesce(func.sum(Transaction.amount), 0))
            .select_from(Account)
            .join(Transaction, Transaction.account_id == Account.id)
            .join(Section, Section.id == Transaction.section_id)
            .where(
                Account.owner_type == AccountOwnerType.ORG,
                Account.owner_id == company_id,
                Section.name == "expense",
                Transaction.direction == TransactionDirection.DEBIT
            )
        ).scalar_one() or Decimal("0")
        
        return total_income - total_expenses
