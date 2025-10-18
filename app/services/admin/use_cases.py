from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from sqlalchemy.orm import Session

from app.services.admin_company import AdminCompanyService
from app.services.admin_dashboard import AdminDashboardService
from app.services.admin_individual import AdminIndividualService


@dataclass
class ListCompanies:
    session: Session

    def execute(
        self,
        *,
        page: int,
        page_size: int,
        search: str | None = None,
    ):
        service = AdminCompanyService(self.session)
        return service.list_companies(page=page, page_size=page_size, search=search)


@dataclass
class GetCompanyDetail:
    session: Session

    def execute(
        self,
        org_id: int,
        *,
        period_key: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        today: date | None = None,
    ):
        service = AdminCompanyService(self.session)
        return service.get_company_detail(
            org_id,
            period_key=period_key,
            start_date=start_date,
            end_date=end_date,
            today=today,
        )


@dataclass
class ListIndividuals:
    session: Session

    def execute(
        self,
        *,
        page: int,
        page_size: int,
        search: str | None = None,
    ):
        service = AdminIndividualService(self.session)
        return service.list_individuals(page=page, page_size=page_size, search=search)


@dataclass
class GetIndividualDetail:
    session: Session

    def execute(
        self,
        user_id: int,
        *,
        period_key: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        today: date | None = None,
    ):
        service = AdminIndividualService(self.session)
        return service.get_individual_detail(
            user_id,
            period_key=period_key,
            start_date=start_date,
            end_date=end_date,
            today=today,
        )

    @staticmethod
    def period_options(*, today: date | None = None):
        return AdminIndividualService.period_options(today=today)


@dataclass
class GetDashboardData:
    session: Session

    def execute(self):
        service = AdminDashboardService(self.session)
        return service.get_dashboard_data()
