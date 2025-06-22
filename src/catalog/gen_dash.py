import logging
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

def chart_query(ind_code: str):
    return f"""
        SELECT report_id, report_name, bc.prd_id, period_name, org_name, hash_id, ind_name, ind_code, ind_unit, tt4, tt5
        FROM public.bao_cao_ktxh_huyen_lac_duong_chi_tieu_thang_7753 bc
        JOIN public.sys_organization o ON bc.org_id = o.id
        JOIN public.rp_input_grant i ON bc.rp_input_grant_id = i.id
        JOIN rp_period p ON p.id = i.period_id
        JOIN rp_report r ON r.id = i.report_id
        WHERE ind_code = '{ind_code}';
    """

def add_filter():
    target_conn = target_engine.connect()

    add_filter = """
    ALTER TABLE catalog.dashboards
    ADD COLUMN IF NOT EXISTS filters JSONB;
    """
    target_conn.execute(text(add_filter))
    target_conn.commit()

def create_chi_tieu_thang_charts():
    source_conn = engine.connect()
    target_conn = target_engine.connect()

    dash_id = settings.CHI_TIEU_THANG

    update_filter = """
        UPDATE catalog.dashboards
        SET filters = jsonb_build_object('fields', :field)
        WHERE id = :id
        """
    target_conn.execute(text(update_filter), {'field': 'prd_id', 'id': dash_id})
    target_conn.commit()

    get_table_name = f"""
        SELECT name, filters
        FROM catalog.dashboards
        WHERE id = {dash_id}
        """

    dashboard_info = pd.read_sql(text(get_table_name), target_conn).iloc[0]
    table_name = dashboard_info['name']
    filters = dashboard_info['filters']

    get_ind_code = f"""
    SELECT DISTINCT ind_code
    FROM public.{table_name}
    """

    list_ind_code = pd.read_sql(text(get_ind_code), source_conn)['ind_code'].tolist()
    filter_col = filters['fields']

    get_filter_value = f"""
    SELECT DISTINCT {filter_col}
    FROM public.{table_name}
    ORDER BY {filter_col}
    """
    list_filter_value = pd.read_sql(text(get_filter_value), source_conn)[filter_col].tolist()
    # print(filter_col)

    chart_service = ChartService(target_engine)
    chart_service.truncate_charts()
    for ind_code in list_ind_code:
        if ind_code == 'Bhxh1':
            query = chart_query(ind_code)
            data = pd.read_sql(query, source_conn)
            for filter_value in list_filter_value:
                filtered_data = data[data[filter_col] == filter_value]
                for _, row in filtered_data.iterrows():
                    title = row['org_name']
                    config = {
                        'value_column': row['tt5'],
                        'threshold_column': row['tt4'],
                    }
                    filters = {
                        filter_col: filter_value,
                    }
                    single_row_data = pd.DataFrame([row])
                    res = chart_service.create_chart(single_row_data, 'dial', title, config, filters)
                    chart_service.save_chart(dash_id, row['hash_id'], row["ind_name"], row["org_name"],
                                             'dial', res['json_data'], res['config'], res['filters'])

        elif ind_code == 'Bhxh3':
            query = chart_query(ind_code)
            data = pd.read_sql(query, source_conn)
            for filter_value in list_filter_value:
                filtered_data = data[data[filter_col] == filter_value]
                title = filtered_data['ind_name'].iloc[0]
                row_id = filtered_data['hash_id'].iloc[0]
                config = {
                    'orientation': 'h',
                    'x_column': 'org_name',
                    'y_column': 'tt5',
                    'x_title': 'Phòng ban',
                    'y_title': 'Cư dân',
                    'unit': filtered_data['ind_unit'].iloc[0],
                }
                filters = {
                    filter_col: filter_value,
                }
                res = chart_service.create_chart(filtered_data, 'bar', title, config, filters)
                chart_service.save_chart(dash_id, row_id , title, title,'bar', res['json_data'], res['config'], res['filters'])

        else:
            query = chart_query(ind_code)
            data = pd.read_sql(query, source_conn)
            for filter_value in list_filter_value:
                filtered_data = data[data[filter_col] == filter_value]
                data_melt = pd.melt(
                    filtered_data,
                    id_vars=['org_name'],
                    value_vars=['tt5', 'tt4'],
                    var_name='Metric',
                    value_name='Value'
                )
                title = filtered_data['ind_name'].iloc[0]
                row_id = filtered_data['hash_id'].iloc[0]
                config = {
                    'orientation': 'v',
                    'x_column': 'org_name',
                    'y_column': 'Value',
                    'color_column': 'Metric',
                    'barmode': 'group',
                    'x_title': 'Phòng ban',
                    'y_title': 'Giá trị',
                    'unit': filtered_data['ind_unit'].iloc[0],
                }
                filters = {
                    filter_col: filter_value,
                }
                res = chart_service.create_chart(data_melt, 'bar', title, config, filters)
                chart_service.save_chart(dash_id, row_id, title, title, 'bar', res['json_data'], res['config'], res['filters'])


if __name__ == "__main__":
    rp_tables = list_rp()
    df_rp = dashboard_info(rp_tables)
    insert_dashboards(df_rp)
    add_filter()
    create_chi_tieu_thang_charts()