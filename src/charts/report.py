import logging

import pandas as pd
from dash import Dash, dcc, html, Input, Output
import plotly.express as px
from sqlalchemy import create_engine

conn = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/postgres")

conn = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/postgres")

# Tải dữ liệu một lần
query = """
        SELECT prd_id, org_name, ind_name, ind_code, tt4
        FROM public.bao_cao_ktxh_huyen_lac_duong_chi_tieu_thang_7753 bc
        JOIN public.sys_organization o ON bc.org_id = o.id
        WHERE ind_code = 'Bhxh1'
        """
df = pd.read_sql(query, conn)
df['prd_id'] = pd.to_datetime(df['prd_id'], format='%Y%m%d')

# Tạo ứng dụng Dash
app = Dash(__name__)
app.title = "Biểu đồ chỉ tiêu KTXH"

app.layout = html.Div([
    html.H2("Tỷ lệ BHYT/BHXH/BHTN theo tổ chức"),

    dcc.Dropdown(
        id='prd-dropdown',
        options=[{'label': d.strftime('%Y-%m'), 'value': d} for d in sorted(df['prd_id'].unique())],
        value=sorted(df['prd_id'].unique())[0],
        clearable=False,
        style={'width': '300px'}
    ),

    dcc.Graph(id='line-chart')
])


@app.callback(
    Output('line-chart', 'figure'),
    Input('prd-dropdown', 'value')
)
def update_chart(selected_prd):
    selected_date = pd.to_datetime(selected_prd)  # CHUYỂN ĐỔI ở đây
    filtered_df = df[df['prd_id'] == selected_date]
    fig = px.bar(
        filtered_df,
        x='org_name',
        y='tt4',
        color='org_name',
        title=f'Tỷ lệ BHYT/BHXH/BHTN cho kỳ {selected_date.strftime("%Y-%m")}',
        labels={'tt4': 'Tỷ lệ (%)', 'org_name': 'Tổ chức'}
    )
    return fig


if __name__ == '__main__':
    app.run(debug=True)