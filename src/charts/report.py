import logging

import pandas as pd
from dash import Dash, dcc, html, Input, Output
import plotly.express as px
from sqlalchemy import create_engine

conn = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/postgres")

# Tải dữ liệu một lần
query = """
        SELECT report_id, report_name, bc.prd_id, period_name, org_name, ind_name, ind_code, ind_unit,tt4 AS target, tt5 AS actual
        FROM public.bao_cao_ktxh_huyen_lac_duong_chi_tieu_thang_7753 bc
        JOIN public.sys_organization o ON bc.org_id = o.id
        JOIN public.rp_input_grant i ON bc.rp_input_grant_id = i.id
        JOIN rp_period p ON p.id = i.period_id
        JOIN rp_report r ON r.id = i.report_id
        WHERE ind_code = 'Bhxh1';
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
    # Chuyển từ wide sang long format
    long_df = filtered_df.melt(
        id_vars=['org_name'],
        value_vars=['actual', 'target'],
        var_name='Loại',
        value_name='Tỷ lệ'
    )

    fig = px.bar(
        long_df,
        x='org_name',
        y='Tỷ lệ',
        color='Loại',
        barmode='group',
        title=f'Tỷ lệ BHYT/BHXH/BHTN cho kỳ {selected_date.strftime("%Y-%m")}',
        labels={'org_name': 'Tổ chức'}
    )
    return fig


if __name__ == '__main__':
    app.run(debug=True)