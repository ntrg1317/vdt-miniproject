import hashlib
import os
import logging
from dataclasses import dataclass
from typing import Optional, List, Tuple, Dict

from src.utils.pg_conn import PostgresConn

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class FieldMetadata:
    """Metadata của trường"""
    name: str
    type: str
    length: int
    business_term: Optional[str]
    demo_values: List[str]
    is_nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    default_value: Optional[str] = None
    position: int = 0

def get_sample_data(database: str, schema: str, table_name: str, limit: int = 3) -> List[Tuple]:
    """Lấy dữ liệu mẫu từ bảng"""
    conn = PostgresConn("source", db=database)
    try:
        sql = f'SELECT * FROM "{schema}"."{table_name}" LIMIT {limit};'
        return conn.select(sql)
    except Exception as e:
        logger.warning(f"Cannot extract data from {schema}.{table_name}: {str(e)}")
        return []
    finally:
        conn.close()

def get_pk_fields(conn: PostgresConn, table_oid: int) -> set:
    try:
        sql = f"""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = {table_oid} AND i.indisprimary;
        """
        result = conn.select(sql)
        return {row[0] for row in result}

    except Exception as e:
        logger.warning(f"Cannot get primary key fields: {e}")
        return set()

def get_fk_fields(conn: PostgresConn, table_oid: int) -> set:
    try:
        sql = f"""
            SELECT a.attname
            FROM pg_constraint c
            JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
            WHERE c.conrelid = {table_oid} AND c.contype = 'f';
        """
        result = conn.select(sql)
        return {row[0] for row in result}
    except Exception as e:
        logger.warning(f"Cannot get foreign key fields: {e}")
        return set()

def get_field_metadata(database: str, table_oid: int, sample_data: List[Tuple]) -> List[FieldMetadata]:
    """Lấy metadata của các trường trong bảng"""
    conn = PostgresConn("source", db=database)
    try:
        sql = f"""
        SELECT a.attname as field_name,
               t.typname as field_type,
               a.attnum as position,
               CASE WHEN attlen > 0 
                    THEN attlen 
                    ELSE CASE WHEN a.atttypmod > 0
                              THEN a.atttypmod - 4
                              ELSE 0
                         END 
               END as field_length,
               d.description as business_term,
               NOT a.attnotnull as is_nullable,
               a.atthasdef as has_default,
               pg_get_expr(ad.adbin, ad.adrelid) as default_value
        FROM pg_attribute a 
        LEFT JOIN pg_type t ON a.atttypid = t.oid
        LEFT JOIN pg_description d ON d.objsubid = a.attnum AND d.objoid = a.attrelid
        LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
        WHERE a.attrelid = {table_oid} 
          AND a.attnum > 0 
          AND a.attname NOT LIKE '%pg.dropped%' 
        ORDER BY a.attnum;
        """

        result = conn.select(sql)
        fields = []

        pk_fields = get_pk_fields(conn, table_oid)
        fk_fields = get_fk_fields(conn, table_oid)

        for row in result:
            position = row[2]

            demo_values = []
            if sample_data:
                for sample_row in sample_data:
                    if position - 1 < len(sample_row):
                        value = sample_row[position - 1]
                        demo_values.append(str(value) if value is not None else "")

            fields.append(FieldMetadata(
                name=row[0],
                type=row[1],
                length=row[3],
                business_term=row[4],
                demo_values=demo_values,
                is_nullable=row[5],
                is_primary_key=row[0] in pk_fields,
                is_foreign_key=row[0] in fk_fields,
                default_value=row[6],
                position=position,
            ))

        return fields

    except Exception as e:
        logger.warning("Cannot fetch field metadata: %s", e)
        return []
    finally:
        conn.close()

def save_field_metadata(table_fields: Dict[str, List[FieldMetadata]], timestamp:str) -> None:
    if not table_fields:
        return
    conn = PostgresConn("target", db="debezium")
    try:
        values = []
        for table_id, fields in table_fields.items():
            for field in fields:
                field_id = hashlib.md5(f"{table_id}.{field.name}".encode()).hexdigest()

                demo_values = field.demo_values + [""] * (3 - len(field.demo_values))

                values.append([
                    field_id, table_id, field.name, field.type, field.length,
                    demo_values[0] if len(demo_values) > 0 else "",
                    demo_values[1] if len(demo_values) > 1 else "",
                    demo_values[2] if len(demo_values) > 2 else "",
                    field.business_term, field.is_nullable, field.is_primary_key,
                    field.is_foreign_key, field.default_value,
                    field.position, timestamp
                ])

        insert_sql = """
            INSERT INTO catalog.field_origin 
            (id, table_id, field, fieldtype, field_length, field_demo, field_demo2, field_demo3, 
             business_term, is_nullable, is_primary_key, is_foreign_key, default_value, position, update_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                table_id = EXCLUDED.table_id,
                field = EXCLUDED.field,
                fieldtype = EXCLUDED.fieldtype,
                field_length = EXCLUDED.field_length,
                field_demo = EXCLUDED.field_demo,
                field_demo2 = EXCLUDED.field_demo2,
                field_demo3 = EXCLUDED.field_demo3,
                business_term = EXCLUDED.business_term,
                is_nullable = EXCLUDED.is_nullable,
                is_primary_key = EXCLUDED.is_primary_key,
                is_foreign_key = EXCLUDED.is_foreign_key,
                default_value = EXCLUDED.default_value,
                position = EXCLUDED.position,
                update_time = EXCLUDED.update_time       
        """

        conn.batch_insert(insert_sql, values)
        logger.info(f"Successfully save %s rows to field metadata:", len(values))
    except Exception as e:
        logger.error(f"Cannot save field metadata: {e}")
        raise e
    finally:
        conn.close()
