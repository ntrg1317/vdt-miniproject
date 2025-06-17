from datetime import datetime
from typing import *
import json
from sqlalchemy import create_engine, MetaData, Table
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

from config.settings import settings


class ChartService:
    def __init__(self, engine):
        self.engine = engine

    def create_chart(self, data: pd.DataFrame, chart_type: str, title: str,
                     config: Optional[Dict[str, Any]] = None,
                     filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if config is None:
            config = {}

        if filters:
            for column, value in filters.items():
                data = data[data[column] == value]

        chart_creators = {
            'bar': self.create_bar_chart,
            'line': self.create_line_chart,
            'pie': self.create_pie_chart,
            'scatter': self.create_scatter_chart,
            'box': self.create_box_chart,
            'histogram': self.create_histogram_chart,
            'heatmap': self.create_heatmap_chart,
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
        x_title = config.get('x_title', x_col)
        y_title = config.get('y_title', y_col)

        fig = px.bar(data, x=x_col, y=y_col, title=title)
        fig.update_layout(
            xaxis_title=config.get('x_title', x_title),
            yaxis_title=config.get('y_title', y_title),
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

    def create_scatter_chart(self, data: pd.DataFrame, title: str, config: Optional[Dict[str, Any]] = None) -> go.Figure:
        x_col = config.get('x_column', data.columns[0])
        y_col = config.get('y_column', data.columns[1] if len(data.columns) > 1 else data.columns[0])

        fig = px.scatter(data, x=x_col, y=y_col, title=title)
        fig.update_layout(
            xaxis_title=config.get('x_title', x_col),
            yaxis_title=config.get('y_title', y_col),
            template='plotly_white'
        )
        return fig

    def create_box_chart(self, data: pd.DataFrame, title: str, config: Optional[Dict[str, Any]] = None) -> go.Figure:
        y_col = config.get('y_column', data.columns[0])
        x_col = config.get('x_column', None)

        if x_col:
            fig = px.box(data, x=x_col, y=y_col, title=title)
        else:
            fig = px.box(data, y=y_col, title=title)

        fig.update_layout(template='plotly_white')
        return fig

    def create_histogram_chart(self, data: pd.DataFrame, title: str, config: Optional[Dict[str, Any]] = None) -> go.Figure:
        x_col = config.get('x_column', data.columns[0])

        fig = px.histogram(data, x=x_col, title=title)
        fig.update_layout(
            xaxis_title=config.get('x_title', x_col),
            yaxis_title='Count',
            template='plotly_white'
        )
        return fig

    def create_heatmap_chart(self, data: pd.DataFrame, title: str, config: Optional[Dict[str, Any]] = None) -> go.Figure:
        if 'pivot_index' in config and 'pivot_columns' in config and 'pivot_values' in config:
            pivot_data = data.pivot_table(
                index=config['pivot_index'],
                columns=config['pivot_columns'],
                values=config['pivot_values'],
                aggfunc='mean'
            )
            fig = px.imshow(pivot_data, title=title, aspect='auto')
        else:
            # Create correlation heatmap for numeric columns
            numeric_data = data.select_dtypes(include=['number'])
            corr_matrix = numeric_data.corr()
            fig = px.imshow(corr_matrix, title=title, aspect='auto')

        fig.update_layout(template='plotly_white')
        return fig

    def save_chart(self, name: str, title: str, chart_type: str, json_data: str,
                     config: Dict[str, Any], filters: Optional[Dict[str, Any]] = None):
        metadata = MetaData()
        charts = Table('catalog.charts', metadata, autoload_with=self.engine)

        stmt = charts.insert().values(
            name=name,
            title=title,
            type=chart_type,
            json_data=json.loads(json_data),
            config=config,
            filters=filters or {},
            created_at=datetime.now()
        ).returning(charts.c.id)

        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            chart_id = result.scalar()

        return chart_id