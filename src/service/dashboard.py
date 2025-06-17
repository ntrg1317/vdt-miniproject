from datetime import datetime
from typing import *

from sqlalchemy import MetaData, select


class DashboardService:
    def __init__(self, engine):
        self.engine = engine
        self.meta = MetaData()
        self.meta.reflect(bind=self.engine)
        self.dashboards = self.meta.tables['dashboards']
        self.charts = self.meta.tables['charts']

    def create_dashboard(self, name: str, description: Optional[str] = None) -> int:
        stmt = self.dashboards.insert().values(
            name=name,
            description=description,
            created_at=datetime.utcnow()
        ).returning(self.dashboards.c.id)

        with self.engine.begin() as conn:
            result = conn.execute(stmt)
            return result.scalar()

    def list_dashboards(self) -> List[Dict[str, Any]]:
        with self.engine.begin() as conn:
            result = conn.execute(select(self.dashboards))
            return [dict(row._mapping) for row in result.fetchall()]

    def get_charts_for_dashboard(self, dashboard_id: int) -> List[Dict[str, Any]]:
        with self.engine.begin() as conn:
            result = conn.execute(
                select(self.charts).where(self.charts.c.dashboard_id == dashboard_id)
            )
            return [dict(row._mapping) for row in result.fetchall()]
