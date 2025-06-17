import hashlib
import os
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List

from src.utils.pg_conn import PostgresConn

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class TableMetadata:
    """Metadata của bảng/view"""
    id: str
    schema: str
    name: str
    rows: int
    size: float
    business_term: Optional[str]
    oid: int
    type: str
    database: str
    frequency: str = "1d"


def describe_databases() -> List[str]:
    conn = PostgresConn("source")
    try:
        sql_get_dbs = f"""
        SELECT datname FROM pg_database WHERE datistemplate = false;
        """
        return conn.select(sql_get_dbs)
    except Exception as e:
        logger.error(e)
        return []
    finally:
        conn.close()

def get_table_metadata(database: str) -> List[TableMetadata]:
    """Lấy metadata của tất cả tables/views trong database"""
    conn = PostgresConn("source", db=database)
    try:
        # Refresh system tables
        conn.execute("ANALYZE VERBOSE;")

        get_table_metadata_query = f"""
        SELECT 
            n.nspname AS schema,
            c.relname AS name,
            c.reltuples::int AS rows,
            ROUND(pg_total_relation_size(c.oid)/1024.0/1024.0, 2) AS size,
            d.description,
            c.oid,
            CASE c.relkind 
                WHEN 'r' THEN 'table'
                WHEN 'm' THEN 'matview'
                WHEN 'v' THEN 'view'
                ELSE c.relkind::text
            END AS type
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND c.relkind IN ('r', 'm', 'v')
        """

        result = conn.select(get_table_metadata_query)

        tables = []
        for row in result:
            table_id = hashlib.md5(f"{database}.{row[0]}.{row[1]}".encode()).hexdigest()

            tables.append(TableMetadata(
                id=table_id,
                schema=row[0],
                name=row[1],
                rows=row[2] or 0,
                size=row[3] or 0.0,
                business_term=row[4],
                oid=row[5],
                type=row[6],
                database=database
            ))

        return tables

    except Exception as e:
        logger.warning("Cannot fetch table metadata: %s", e)
        logger.warning(
            "Please check if the database '%s' exists and you have access to it",
            database
        )
        return []
    finally:
        conn.close()

def save_table_metadata(tables: List[TableMetadata], timestamp:str) -> None:
    if not tables:
        return

    conn = PostgresConn("target", db="debezium")
    datasource = conn.get_datasource()
    try:
        values = []

        for table in tables:
            values.append([
                table.id, datasource, table.database, table.schema, table.name,
                table.business_term, table.frequency, table.rows, table.size, table.type,
                timestamp, False, False
            ])

        insert_sql = """
            INSERT INTO catalog.table_origin (id, datasource, database, schema, tablename,
                                      business_term, frequency, rows, size, type,
                                      update_time, skip, expire)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                datasource = EXCLUDED.datasource,
                database = EXCLUDED.database,
                schema = EXCLUDED.schema,
                tablename = EXCLUDED.tablename,
                business_term = EXCLUDED.business_term,
                frequency = EXCLUDED.frequency,
                rows = EXCLUDED.rows,
                size = EXCLUDED.size,
                type = EXCLUDED.type,
                update_time = EXCLUDED.update_time,
                skip = EXCLUDED.skip,
                expire = EXCLUDED.expire
        """

        conn.batch_insert(insert_sql, values)
        logger.info(f"Successfully save %s rows to table metadata:", len(values))


    except Exception as e:
        logger.error(f"Cannot save table metadata: {e}")
    finally:
        conn.close()

def collect_table_metadata():
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Collecting table metadata at {timestamp}")

    conn = PostgresConn("source", db="postgres")
    databases = describe_databases(conn)
    for database in databases:
        logger.info(f"Processing database: {database[0]}")
        tables = get_table_metadata(database[0])
        save_table_metadata(tables, timestamp)