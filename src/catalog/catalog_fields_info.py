import hashlib
import os
import logging
from dataclasses import dataclass
from typing import Optional, List, Tuple

from src.utils.pg_conn import PostgresConn

@dataclass
class FieldMetadata:
    """Metadata của trường"""
    name: str
    type: str
    length: int
    business_term: Optional[str]
    demo_values: List[str]

def get_sample_data(database: str, schema: str, table_name: str, limit: int = 3) -> List[Tuple]:
    """Lấy dữ liệu mẫu từ bảng"""
    conn = connect_db("source", database)
    try:
        sql = f'SELECT * FROM "{schema}"."{table_name}" LIMIT {limit};'
        return conn.select(sql)
    except Exception as e:
        logger.warning(f"Không thể lấy sample data từ {schema}.{table_name}: {str(e)}")
        return []
    finally:
        conn.close()

def get_field_metadata(self, database: str, table_oid: int, sample_data: List[Tuple]) -> List[FieldMetadata]:
    """Lấy metadata của các trường trong bảng"""
    conn = connect_db("source", database)
    try:
        sql = f"""
        SELECT a.attname as field_name,
               t.typname as field_type,
               a.attnum,
               CASE WHEN attlen > 0 
                    THEN attlen 
                    ELSE CASE WHEN a.atttypmod > 0
                              THEN a.atttypmod - 4
                              ELSE 0
                         END 
               END as field_length,
               d.description as business_term
        FROM pg_attribute a 
        LEFT JOIN pg_type t ON a.atttypid = t.oid
        LEFT JOIN pg_description d ON d.objsubid = a.attnum AND d.objoid = a.attrelid
        WHERE a.attrelid = {table_oid} 
          AND a.attnum > 0 
          AND a.attname NOT LIKE '%pg.dropped%' 
        ORDER BY a.attnum;
        """

        result = conn.select(sql)
        fields = []

        for idx, row in enumerate(result):
            demo_values = []
            for sample_row in sample_data:
                if idx < len(sample_row):
                    demo_values.append(str(sample_row[idx]))

            fields.append(FieldMetadata(
                name=row[0],
                type=row[1],
                length=row[3],
                business_term=row[4],
                demo_values=demo_values
            ))

        return fields

    except Exception as e:
        self.logger.warn("Cannot fetch field metadata: %s", e)
        return []
    finally:
        conn.close()