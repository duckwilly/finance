"""ORM model for linking individuals to organisations."""
from __future__ import annotations

from sqlalchemy import BigInteger, ForeignKey, Integer, String, select
from sqlalchemy.orm import Mapped, mapped_column, Session

from .base import Base
from .transactions import Account, Transaction, Section, Category, AccountOwnerType
from .companies import Company

_ID_TYPE = BigInteger().with_variant(Integer, "sqlite")


class Membership(Base):
    """Associates an individual with an employer/company."""

    __tablename__ = "membership"

    id: Mapped[int] = mapped_column(_ID_TYPE, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("user.id"), nullable=False, index=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("org.id"), nullable=False, index=True)
    role: Mapped[str] = mapped_column(String(64), nullable=False, server_default="member")

    @staticmethod
    def find_employer_for_user(session: Session, user_id: int) -> Company | None:
        """
        Find the employer company for a given user by analyzing salary transactions.
        
        This method looks for salary transactions (section='income', category='Salary') 
        in the user's accounts and identifies the employer company from the transaction description.
        
        Args:
            session: Database session
            user_id: ID of the user to find employer for
            
        Returns:
            Company object if employer found, None otherwise
        """
        salary_query = (
            select(Transaction.description)
            .join(Account, Account.id == Transaction.account_id)
            .join(Section, Section.id == Transaction.section_id)
            .outerjoin(Category, Category.id == Transaction.category_id)
            .where(
                Account.owner_type == "user",
                Account.owner_id == user_id,
                Section.name == "income",
                Category.name == "Salary"
            )
            .limit(1)
        )
        
        description = session.execute(salary_query).scalar_one_or_none()
        
        if not description or not description.startswith("Salary from "):
            return None
            
        # Extract company name from description (format: "Salary from Company Name")
        company_name = description.replace("Salary from ", "").strip()
        
        # Find the company by name
        company_query = select(Company).where(Company.name == company_name)
        return session.execute(company_query).scalar_one_or_none()
