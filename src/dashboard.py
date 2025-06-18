import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import json
from sqlalchemy import create_engine
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config.settings import settings
from src.utils.chatbot import *

# Streamlit page config
st.set_page_config(
    page_title="Dashboard Charts",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)


# Database connection
@st.cache_resource
def init_connection():
    engine = create_engine(settings.target_database_url)
    return engine

# Load dashboard data
@st.cache_data
def load_dashboards():
    engine = init_connection()
    query = """
            SELECT DISTINCT dashboard_id,
                            COUNT(*)        as chart_count,
                            MIN(created_at) as created_at
            FROM catalog.charts
            GROUP BY dashboard_id
            ORDER BY dashboard_id DESC \
            """
    return pd.read_sql(query, engine)


# Load charts for dashboard
@st.cache_data
def load_charts(dashboard_id):
    engine = init_connection()
    query = """
            SELECT id, \
                   dashboard_id, \
                   name, \
                   title, \
                   type, \
                   config, \
                   filters, \
                   json_data, \
                   created_at
            FROM catalog.charts
            WHERE dashboard_id = %(dashboard_id)s
            ORDER BY created_at DESC \
            """
    return pd.read_sql(query, engine, params={'dashboard_id': 7753})


# Create plotly figure from json data
def create_figure_from_json(json_data):
    try:
        fig = go.Figure(json_data)
        return fig
    except Exception as e:
        st.error(f"Error creating figure: {str(e)}")
        return None

def parse_filters(row):
    try:
        return json.loads(row) if isinstance(row, str) else row
    except Exception:
        return {}

# Main app
def main():
    st.title("📊 Dashboard Charts Viewer")
    st.markdown("---")

    # Load metadata từ database
    try:
        engine = init_connection()
        metadata_df = load_metadata(engine)

        st.sidebar.header("🧠 Metadata Chatbot")
        user_question = st.sidebar.text_input("Hỏi về bảng/cột dữ liệu:")

        if user_question:
            with st.spinner("Đang trả lời..."):
                response = ask_metadata_bot(user_question, metadata_df)
                st.sidebar.success(response)

    except Exception as e:
        st.sidebar.error(f"Lỗi khi tải metadata/chatbot: {str(e)}")

    # Sidebar for dashboard selection
    st.sidebar.header("Dashboard Selection")

    # Load dashboards
    try:
        dashboards = load_dashboards()

        if dashboards.empty:
            st.warning("No dashboards found")
            return

        # Dashboard selector
        dashboard_options = {}
        for _, row in dashboards.iterrows():
            label = f"Dashboard {row['dashboard_id']} ({row['chart_count']} charts)"
            dashboard_options[label] = row['dashboard_id']

        selected_dashboard_label = st.sidebar.selectbox(
            "Select Dashboard",
            options=list(dashboard_options.keys())
        )

        selected_dashboard_id = dashboard_options[selected_dashboard_label]

        # Dashboard info
        dashboard_info = dashboards[dashboards['dashboard_id'] == selected_dashboard_id].iloc[0]

        st.sidebar.markdown("### Dashboard Info")
        st.sidebar.write(f"**ID:** {dashboard_info['dashboard_id']}")
        st.sidebar.write(f"**Charts:** {dashboard_info['chart_count']}")
        st.sidebar.write(f"**Created:** {dashboard_info['created_at'].strftime('%Y-%m-%d %H:%M')}")

        # Refresh button
        if st.sidebar.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()

    except Exception as e:
        st.error(f"Error loading dashboards: {str(e)}")
        return

    # Main content area
    st.header(f"Dashboard {selected_dashboard_id} Charts")

    # Load charts for selected dashboard
    try:
        charts = load_charts(selected_dashboard_id)

        st.sidebar.header("Chart Filters")

        charts["filters_dict"] = charts["filters"].apply(parse_filters)
        available_prds = sorted({
            f["prd_id"]
            for f in charts["filters_dict"]
            if isinstance(f, dict) and "prd_id" in f
        })

        selected_prd = st.sidebar.selectbox("Filter by prd_id", available_prds)

        available_types = sorted(charts['type'].dropna().unique().tolist())
        selected_types = st.sidebar.multiselect("Filter by Type", options=available_types, default=available_types)

        available_names = sorted(charts['name'].dropna().unique().tolist())
        selected_names = st.sidebar.multiselect("Filter by Name", options=available_names, default=available_names)


        # Apply filters
        filtered_charts = charts[
            charts['filters_dict'].apply(lambda x: x.get("prd_id") == selected_prd) &
            charts['type'].isin(selected_types) &
            charts['name'].isin(selected_names)
            ]

        # Replace charts with filtered_charts
        charts = filtered_charts

        if charts.empty:
            st.warning("No charts found for this dashboard")
            return

        # Display options
        col1, col2, col3 = st.columns([2, 1, 1])

        with col1:
            view_mode = st.selectbox(
                "View Mode",
                ["Grid View", "List View", "Full Screen"]
            )

        with col2:
            charts_per_row = st.selectbox(
                "Charts per Row",
                [1, 2, 3, 4],
                index=1
            )

        with col3:
            show_details = st.checkbox("Show Chart Details", value=False)

        st.markdown("---")

        # Display charts based on view mode
        if view_mode == "Grid View":
            display_grid_view(charts, charts_per_row, show_details)
        elif view_mode == "List View":
            display_list_view(charts, show_details)
        else:  # Full Screen
            display_fullscreen_view(charts, show_details)

    except Exception as e:
        st.error(f"Error loading charts: {str(e)}")


def display_grid_view(charts, charts_per_row, show_details):
    """Display charts in grid layout"""
    for i in range(0, len(charts), charts_per_row):
        cols = st.columns(charts_per_row)

        for j, col in enumerate(cols):
            if i + j < len(charts):
                chart = charts.iloc[i + j]

                with col:
                    display_single_chart(chart, show_details)


def display_list_view(charts, show_details):
    """Display charts in list layout"""
    for _, chart in charts.iterrows():
        with st.container():
            display_single_chart(chart, show_details)
            st.markdown("---")


def display_fullscreen_view(charts, show_details):
    """Display charts in full screen mode"""
    # Chart selector
    chart_options = {}
    for _, chart in charts.iterrows():
        label = f"{chart['title']} ({chart['type']})"
        chart_options[label] = chart['id']

    selected_chart_label = st.selectbox(
        "Select Chart",
        options=list(chart_options.keys())
    )

    selected_chart_id = chart_options[selected_chart_label]
    selected_chart = charts[charts['id'] == selected_chart_id].iloc[0]

    display_single_chart(selected_chart, show_details, fullscreen=True)


def display_single_chart(chart, show_details=False, fullscreen=False):
    """Display a single chart with optional details"""

    # Chart container
    if not fullscreen:
        st.subheader(chart['title'])
    else:
        st.title(chart['title'])

    # Create and display figure
    fig = create_figure_from_json(chart['json_data'])

    if fig:
        height = 600 if fullscreen else 400
        st.plotly_chart(fig, use_container_width=True, height=height)
    else:
        st.error("Could not render chart")

    # Show details if requested
    if show_details:
        with st.expander("Chart Details"):
            col1, col2 = st.columns(2)

            with col1:
                st.write("**Basic Info:**")
                st.write(f"- ID: {chart['id']}")
                st.write(f"- Type: {chart['type']}")
                st.write(f"- Created: {chart['created_at']}")

            with col2:
                st.write("**Configuration:**")
                try:
                    config = json.loads(chart['config'])
                    for key, value in config.items():
                        st.write(f"- {key}: {value}")
                except:
                    st.write("Could not parse configuration")

            # Filters
            if chart['filters'] and chart['filters'] != '{}':
                st.write("**Filters:**")
                try:
                    filters = json.loads(chart['filters'])
                    for key, value in filters.items():
                        st.write(f"- {key}: {value}")
                except:
                    st.write("Could not parse filters")


# Export functionality
def add_export_functionality():
    """Add export options to sidebar"""
    st.sidebar.markdown("---")
    st.sidebar.header("Export Options")

    if st.sidebar.button("📊 Export Dashboard PDF"):
        st.sidebar.info("PDF export functionality can be added here")

    if st.sidebar.button("📈 Export All Charts"):
        st.sidebar.info("Bulk export functionality can be added here")


# Run the app
if __name__ == "__main__":
    main()
    add_export_functionality()