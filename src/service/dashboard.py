from datetime import datetime
from typing import *

from sqlalchemy import MetaData, select
from sqlalchemy.dialects.postgresql import insert


class DashboardService:
    def __init__(self, engine):
        self.engine = engine
        self.meta = MetaData()
        self.meta.reflect(bind=self.engine, schema="catalog")
        self.dashboards = self.meta.tables['catalog.dashboards']
        self.charts = self.meta.tables['catalog.charts']

    def create_dashboard(self, id, name: str, description: Optional[str] = None):
        stmt = insert(self.dashboards).values(
            id=id,
            name=name,
            description=description,
            created_at=datetime.now()
        ).on_conflict_do_update(
            index_elements=['id'],  # cột bị xung đột
            set_={
                'name': name,
                'description': description,
                'created_at': datetime.now()
            }
        )

        with self.engine.begin() as conn:
            conn.execute(stmt)

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
