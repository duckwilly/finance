from datetime import date
from decimal import Decimal
from unittest.mock import create_autospec

from sqlalchemy.orm import Session

from app.repositories.admin.company_repository import (
    AdminCompanyRepository,
    CashflowRow,
    CompanyAccountRow,
    CompanyMemberRow,
    CompanyRow,
    CompanySummaryRow,
    ExpenseCategoryRow,
    IncomeTransactionRow,
    PayrollEmployeeRow,
)
from app.services.admin_company import AdminCompanyService


def _service_with_repository(repository: AdminCompanyRepository) -> AdminCompanyService:
    session = create_autospec(Session, instance=True)
    return AdminCompanyService(session, repository=repository)


def test_pagination_bounds() -> None:
    """Pages beyond the dataset collapse to the last available page."""

    repository = create_autospec(AdminCompanyRepository, instance=True)
    repository.count_companies.return_value = 23
    repository.fetch_company_summaries.return_value = [
        CompanySummaryRow(org_id=21, org_name="Org 21", total_balance=Decimal("0"), payroll_headcount=0),
        CompanySummaryRow(org_id=22, org_name="Org 22", total_balance=Decimal("0"), payroll_headcount=0),
        CompanySummaryRow(org_id=23, org_name="Org 23", total_balance=Decimal("0"), payroll_headcount=0),
    ]

    service = _service_with_repository(repository)

    page = service.list_companies(page=5, page_size=10)

    assert page.total == 23
    assert page.page == 3
    assert page.page_size == 10
    assert len(page.items) == 3
    assert all(company.total_balance == Decimal("0") for company in page.items)

    repository.fetch_company_summaries.assert_called_once_with(search=None, limit=10, offset=20)


def test_metrics_with_payroll_mix() -> None:
    """Payroll headcount and balances aggregate correctly across accounts."""

    repository = create_autospec(AdminCompanyRepository, instance=True)
    repository.count_companies.side_effect = [2, 1]
    repository.fetch_company_summaries.side_effect = [
        [
            CompanySummaryRow(
                org_id=1,
                org_name="Acme Corp",
                total_balance=Decimal("440"),
                payroll_headcount=2,
            ),
            CompanySummaryRow(
                org_id=2,
                org_name="Globex LLC",
                total_balance=Decimal("500"),
                payroll_headcount=2,
            ),
        ],
        [
            CompanySummaryRow(
                org_id=2,
                org_name="Globex LLC",
                total_balance=Decimal("500"),
                payroll_headcount=2,
            )
        ],
    ]

    service = _service_with_repository(repository)

    page = service.list_companies(page=1, page_size=10)
    by_name = {company.name: company for company in page.items}

    acme = by_name["Acme Corp"]
    globex = by_name["Globex LLC"]

    assert acme.total_balance == Decimal("440")
    assert acme.payroll_headcount == 2

    assert globex.total_balance == Decimal("500")
    assert globex.payroll_headcount == 2

    filtered = service.list_companies(page=1, page_size=10, search="globex")
    assert filtered.total == 1
    assert filtered.items[0].name == "Globex LLC"

    first_call = repository.fetch_company_summaries.call_args_list[0]
    assert first_call.kwargs == {"search": None, "limit": 10, "offset": 0}

    second_call = repository.fetch_company_summaries.call_args_list[1]
    assert second_call.kwargs == {"search": "globex", "limit": 10, "offset": 0}


def test_get_company_detail() -> None:
    """Detailed view returns account and membership metadata."""

    repository = create_autospec(AdminCompanyRepository, instance=True)
    repository.get_company.side_effect = [
        CompanyRow(org_id=1, org_name="Acme Corp"),
        None,
    ]
    repository.list_accounts.return_value = [
        CompanyAccountRow(
            account_id=1,
            account_name="Operating Account",
            account_type="operating",
            account_currency="EUR",
            balance=Decimal("650"),
        ),
        CompanyAccountRow(
            account_id=2,
            account_name=None,
            account_type="savings",
            account_currency="EUR",
            balance=Decimal("-90"),
        ),
    ]
    repository.get_payroll_headcount.return_value = 2
    repository.list_members.return_value = [
        CompanyMemberRow(
            user_id=1,
            user_name="Dana Carvey",
            user_email="dana@example.com",
            membership_role="OWNER",
        ),
        CompanyMemberRow(
            user_id=2,
            user_name="Lee Jordan",
            user_email=None,
            membership_role="ACCOUNTANT",
        ),
    ]
    repository.get_cashflow.return_value = CashflowRow(
        income_total=Decimal("1000"),
        expense_total=Decimal("440"),
    )
    repository.list_income_transactions.return_value = [
        IncomeTransactionRow(txn_date=date(2024, 1, 10), normalized_amount=Decimal("1000")),
    ]
    repository.list_payroll_employees.return_value = [
        PayrollEmployeeRow(counterparty_id=2, counterparty_name="Bob", total_paid=Decimal("240")),
        PayrollEmployeeRow(counterparty_id=1, counterparty_name="Alice", total_paid=Decimal("200")),
    ]
    repository.list_top_expense_categories.return_value = [
        ExpenseCategoryRow(category_id=1, category_name="Payroll", total_spent=Decimal("350")),
        ExpenseCategoryRow(category_id=2, category_name="Salary Bonus", total_spent=Decimal("90")),
    ]

    service = _service_with_repository(repository)

    detail = service.get_company_detail(1, today=date(2024, 1, 31))

    assert detail is not None
    assert detail.name == "Acme Corp"
    assert detail.total_balance == Decimal("560")
    assert detail.payroll_headcount == 2
    assert detail.period.key == "ytd"
    assert detail.income_total == Decimal("1000")
    assert detail.expense_total == Decimal("440")
    assert detail.net_cash_flow == Decimal("560")
    assert detail.payroll_total == Decimal("440")
    assert {account.account_id for account in detail.accounts} == {1, 2}

    operating = next(account for account in detail.accounts if account.account_id == 1)
    assert operating.name == "Operating Account"
    assert operating.balance == Decimal("650")

    savings = next(account for account in detail.accounts if account.account_id == 2)
    assert savings.name is None
    assert savings.balance == Decimal("-90")

    assert {member.name for member in detail.members} == {"Dana Carvey", "Lee Jordan"}
    owner = next(member for member in detail.members if member.name == "Dana Carvey")
    assert owner.role == "OWNER"
    assert owner.email == "dana@example.com"

    assert [employee.name for employee in detail.payroll_employees] == ["Bob", "Alice"]
    bob = detail.payroll_employees[0]
    assert bob.total_compensation == Decimal("240")
    alice = detail.payroll_employees[1]
    assert alice.total_compensation == Decimal("200")

    assert [category.name for category in detail.top_expense_categories] == [
        "Payroll",
        "Salary Bonus",
    ]
    assert detail.top_expense_categories[0].total_spent == Decimal("350")
    assert detail.top_expense_categories[1].total_spent == Decimal("90")

    assert service.get_company_detail(999) is None

