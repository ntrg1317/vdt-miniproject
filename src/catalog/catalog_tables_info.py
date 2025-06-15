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
    schema: str
    name: str
    rows: int
    size: float
    business_term: Optional[str]
    oid: int
    type: str
    database: str
    frequency: str = "1d"


def describe_databases(conn: PostgresConn) -> List[str]:
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
        conn.select("ANALYZE VERBOSE;")

        get_table_metadata_query = f"""
        SELECT 
            schemaname, 
            tablename, 
            CAST(reltuples AS int4) AS row,
            ROUND(CAST(relpages/128.0 AS numeric), 2) AS size, 
            CAST(d.description AS TEXT), 
            c.oid, 
            'table' AS objecttype
        FROM pg_class c
        LEFT JOIN pg_namespace n ON c.relnamespace = n.oid
        LEFT JOIN pg_tables t ON (n.nspname = t.schemaname AND c.relname = t.tablename)
        LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0
        WHERE c.relkind = 'r' 
          AND schemaname NOT IN ('information_schema','pg_catalog')
        
        UNION
        
        SELECT 
            v.schemaname, 
            v.matviewname, 
            0, 
            0.0, 
            v.definition, 
            c.oid, 
            'matview' AS object_type
        FROM pg_class c
        LEFT JOIN pg_namespace n ON c.relnamespace = n.oid
        LEFT JOIN pg_matviews v ON (n.nspname = v.schemaname AND c.relname = v.matviewname)
        WHERE schemaname NOT IN ('information_schema','pg_catalog')
        
        UNION
        
        SELECT 
            v.schemaname, 
            v.viewname, 
            0, 
            0.0, 
            v.definition, 
            c.oid, 
            'view' AS object_type
        FROM pg_class c
        LEFT JOIN pg_namespace n ON c.relnamespace = n.oid
        LEFT JOIN pg_views v ON (n.nspname = v.schemaname AND c.relname = v.viewname)
        WHERE schemaname NOT IN ('information_schema','pg_catalog');
        """

        result = conn.select(get_table_metadata_query)
        logger.info(result)

        tables = []
        for row in result:
            tables.append(TableMetadata(
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
        conn.truncate("catalog.table_origin")
        for table in tables:
            values = []

            table_id = hashlib.md5(f"{table.database}.{table.schema}.{table.name}".encode()).hexdigest()
            values.append([
                table_id, datasource, table.database, table.schema, table.name,
                table.business_term, table.frequency, table.rows, table.size, table.type,
                timestamp, False, False
            ])

        insert_sql = """
                     INSERT INTO catalog.table_origin (id, datasource, database, schema, tablename,
                                               business_term, frequency, rows, size, type,
                                               update_time, skip, expire)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s); \
                     """

        conn.batch_insert(insert_sql, values)
        logger.info(f"Successfully save % rows to table metadata:", len(values))


    except Exception as e:
        logger.error(f"Cannot save table metadata: {e}")
    finally:
        conn.close()

def collect_metadata():
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Collecting table metadata at {timestamp}")

    conn = PostgresConn("source", db="postgres")
    databases = describe_databases(conn)
    for database in databases:
        tables = get_table_metadata(database[0])
        save_table_metadata(tables, timestamp)

    logger.info("Finished collecting table metadata")

def main():
    collect_metadata()

if __name__ == "__main__":
    main()
