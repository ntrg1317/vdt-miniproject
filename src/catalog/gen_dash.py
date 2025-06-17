from typing import List, Tuple

import pandas as pd
from sqlalchemy import create_engine, text

from config.settings import settings
from src.service.dashboard import DashboardService
from src.service.chart import ChartService

engine = create_engine(settings.database_url)
target_engine = create_engine(settings.target_database_url)

def list_rp() -> List[str]:
    get_rp = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE'
      AND table_name ILIKE 'bao_cao%';
    """

    tables = pd.read_sql(text(get_rp), engine)

    list_tables =  tables['table_name'].tolist()

    return list_tables

def dashboard_info(tables: List[str]) -> pd.DataFrame:
    id_to_table = {}

    ids = []
    for table in tables:
        suffix = table.split('_')[-1]
        if suffix.isdigit():
            ids.append(int(suffix))
            id_to_table[int(suffix)] = table

    if not ids:
        return pd.DataFrame()

    query = text("""
                 SELECT id as report_id, report_name
                 FROM public.rp_report
                 WHERE id = ANY (:ids)
                 """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"ids": ids})

    df['table'] = df['report_id'].map(id_to_table)

    return df

def insert_dashboards(df_dash: pd.DataFrame):
    if df_dash.empty:
        print("No dashboards to insert.")
        return
    df_dash = df_dash.rename(columns={'report_id': 'id', 'table': 'name', 'report_name': 'description'})
    for _, row in df_dash.iterrows():
        dash = DashboardService(target_engine)
        dash.create_dashboard(row['id'], row['name'], row['description'])

def create_chi_tieu_thang_charts():
    conn = target_engine.connect()

    dash_id = settings.CHI_TIEU_THANG
    get_table_name = f"""
    SELECT name, filters
    FROM catalog.dashboards
    WHERE id = {dash_id}
    """

    table_name = pd.read_sql(text(get_table_name), conn)['name'].iloc[0]
    filters = pd.read_sql(text(get_table_name), conn)['filters'].iloc[0]

    get_ind_code = f"""
    SELECT DISTINCT ind_code
    FROM {table_name}
    """

    list_ind_code = pd.read_sql(text(get_ind_code), conn)['ind_name'].tolist()
    filter_col = filters['fields']

    get_filter_value = f"""
    SELECT DISTINCT {filter_col}
    FROM {table_name}
    ORDER BY {filter_col}
    """
    list_filter_value = pd.read_sql(text(get_filter_value), conn)[filter_col].tolist()

    for ind_code in list_ind_code:
        if ind_code == 'Bhxh1'



if __name__ == "__main__":
    rp_tables = list_rp()
    df_rp = dashboard_info(rp_tables)
    insert_dashboards(df_rp)

    query = f"""
    SELECT report_id, report_name, bc.prd_id, period_name, org_name, ind_name, ind_code, ind_unit,tt4 AS target, tt5 AS actual
    FROM public.bao_cao_ktxh_huyen_lac_duong_chi_tieu_thang_7753 bc
    JOIN public.sys_organization o ON bc.org_id = o.id
    JOIN public.rp_input_grant i ON bc.rp_input_grant_id = i.id
    JOIN rp_period p ON p.id = i.period_id
    JOIN rp_report r ON r.id = i.report_id
    WHERE ind_code = 'Bhxh1';
    """

    df = pd.read_sql(query, engine)
    df['prd_id'] = pd.to_datetime(df['prd_id'], format='%Y%m%d')

    df_long = df.melt(
        id_vars=['org_name'],
        value_vars=['target', 'actual'],
        var_name='Loại',
        value_name='Giá trị'
    )

    title = df['ind_name'].iloc[0]
    config = {
        'x_column': 'org_name',
        'y_column': 'actual',
        'x_title': 'Phòng ban',
        'y_title': "Thực tế"
    }

    chart_service = ChartService(target_engine)
    result = chart_service.create_chart(df, 'bar', title, config)
    result['figure'].show()
    # print(result['json_data'])