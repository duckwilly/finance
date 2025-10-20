"""ORM model for precomputed monthly salary per user and employer."""
from __future__ import annotations

from sqlalchemy import BigInteger, Integer, Numeric
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class UserSalaryMonthly(Base):
    """Pre-aggregated monthly salary for a user and employer.

    Mirrors the user_salary_monthly table defined in sql/schema.sql.
    """

    __tablename__ = "user_salary_monthly"

    user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    employer_org_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    year: Mapped[int] = mapped_column(Integer, primary_key=True)
    month: Mapped[int] = mapped_column(Integer, primary_key=True)
    salary_amount: Mapped[float] = mapped_column(Numeric(18, 4), nullable=False)


