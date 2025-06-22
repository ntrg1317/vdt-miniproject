from datetime import datetime
from typing import *
import json
from sqlalchemy import create_engine, MetaData, Table, text
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from sqlalchemy.dialects.postgresql import insert

from config.settings import settings


class ChartService:
    def __init__(self, engine):
        self.engine = engine
        self.meta = MetaData()
        self.meta.reflect(bind=self.engine, schema="catalog")
        self.charts = self.meta.tables['catalog.charts']

    def create_chart(self, data: pd.DataFrame, chart_type: str, title: str,
                     config: Optional[Dict[str, Any]] = None,
                     filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if config is None:
            config = {}

        # if filters:
        #     for column, value in filters.items():
        #         data = data[data[column] == value]

        chart_creators = {
            'bar': self.create_bar_chart,
            'line': self.create_line_chart,
            'pie': self.create_pie_chart,
            'dial': self.create_dial_chart,
        }

        if chart_type not in chart_creators:
            raise ValueError(f'Unsupported chart type: {chart_type}')

        fig = chart_creators[chart_type](data, title, config)

        chart_json = fig.to_json()

        return {
            'figure': fig,
            'json_data': chart_json,
            'config': config,
            'filters': filters,
        }

    def create_bar_chart(self, data: pd.DataFrame, title: str, config: Dict[str, Any]) -> go.Figure:
        x_col = config.get('x_column', data.columns[0])
        y_col = config.get('y_column', data.columns[1] if len(data.columns) > 1 else data.columns[0])
        color_col = config.get('color_column')  # for grouped/stacked bars
        orientation = config.get('orientation', 'v')  # 'v' (vertical) or 'h' (horizontal)
        barmode = config.get('barmode', 'group')  # 'group' or 'stack'
        x_title = config.get('x_title', x_col)
        y_title = config.get('y_title', y_col) + ' (' + config.get('unit', '') + ')'

        if orientation not in ['v', 'h']:
            orientation = 'v'

        if orientation == 'h':
            x_axis = y_col
            y_axis = x_col
            x_title = x_title
            y_title = y_title
        else:
            x_axis = x_col
            y_axis = y_col
            x_title = x_title
            y_title = y_title

            # Create the bar chart
        fig = px.bar(
            data,
            x=x_axis,
            y=y_axis,
            color=color_col if color_col and color_col in data.columns else None,
            title=title,
            orientation=orientation
        )

        # Apply layout settings
        fig.update_layout(
            barmode=barmode,  # 'group', 'stack', or 'relative'
            xaxis_title=x_title,
            yaxis_title=y_title,
            template='plotly_white'
        )

        return fig


    def create_line_chart(self, data: pd.DataFrame, title: str, config: Optional[Dict[str, Any]] = None) -> go.Figure:
        x_col = config.get('x_column', data.columns[0])
        y_col = config.get('y_column', data.columns[1] if len(data.columns) > 1 else data.columns[0])

        fig = px.line(data, x=x_col, y=y_col, title=title)
        fig.update_layout(
            xaxis_title=config.get('x_title', x_col),
            yaxis_title=config.get('y_title', y_col),
            template='plotly_white'
        )
        return fig

    def create_pie_chart(self, data: pd.DataFrame, title: str, config: Optional[Dict[str, Any]] = None) -> go.Figure:
        names_col = config.get('names_column', data.columns[0])
        values_col = config.get('values_column', data.columns[1] if len(data.columns) > 1 else data.columns[0])

        fig = px.pie(data, names=names_col, values=values_col, title=title)
        fig.update_layout(template='plotly_white')
        return fig

    def create_dial_chart(self, data: pd.DataFrame, title: str, config: Optional[Dict[str, Any]] = None) -> go.Figure:
        value_col = config.get('value_column', data.columns[0])
        threshold_col = config.get('threshold_column', data.columns[1] if len(data.columns) > 1 else data.columns[0])

        fig = go.Figure(go.Indicator(
            mode = "gauge+number",
            value = value_col,
            title = {"text": title},
            domain = {"x": [0, 1], "y": [0, 1]},
            gauge = {
                "axis": {"range": [0, 100]},
                "bar": {"color": "darkblue"},
                "bgcolor": "white",
                "borderwidth": 2,
                "bordercolor": "gray",
                'steps': [
                    {'range': [0, 25], 'color': "lightgray"},
                    {'range': [25, 75], 'color': "gray"},
                    {'range': [75, 100], 'color': "darkgray"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': threshold_col
                }
            }
        ))

        # Add invisible scatter trace for threshold tooltip
        fig.add_annotation(
            x=0.5,
            y=-0.15,
            text=f"Target: {threshold_col}",
            showarrow=False,
            font=dict(size=14, color="red"),
            xref="paper",
            yref="paper"
        )

        fig.update_layout(
            template='plotly_white',
            height=400
        )

        return fig

    def save_chart(self, dashboard_id:int, row_id:str, name: str, title: str, chart_type: str, json_data: str,
                     config: Dict[str, Any], filters: Optional[Dict[str, Any]] = None):

        stmt = insert(self.charts).values(
            dashboard_id=dashboard_id,
            row_id=row_id,
            name=name,
            title=title,
            type=chart_type,
            json_data=json.loads(json_data),
            config=config,
            filters=filters or {},
            created_at=datetime.now()
        ).on_conflict_do_update(
            index_elements=['dashboard_id', 'row_id'],  # cột bị xung đột
            set_={
                'dashboard_id': dashboard_id,
                'row_id': row_id,
                'name': name,
                'title': title,
                'type': chart_type,
                'json_data': json.loads(json_data),
                'config': config,
                'filters': filters or {},
                'created_at': datetime.now()
            }
        )

        try:
            with self.engine.connect() as conn:
                conn.execute(stmt)
                conn.commit()
        except Exception as e:
            print(f"Error saving chart: {e}")
            raise e

    def truncate_charts(self):
        with self.engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {self.charts} RESTART IDENTITY"))
            conn.commit()

    def get_data_row_id(self, row_id: str):
        query = text("""
                     SELECT *
                     FROM catalog.charts
                     WHERE row_id = :row_id
                     """)
        with self.engine.connect() as conn:
            result = conn.execute(query, {"row_id": row_id}).mappings().fetchone()
            return dict(result) if result else None