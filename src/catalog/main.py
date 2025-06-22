from datetime import datetime

from src.catalog.catalog_fields_info import *
from src.catalog.catalog_tables_info import *


def collect_metadata():
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Collecting table metadata at {timestamp}")

    databases = describe_databases()

    all_tables = []
    all_table_fields = {}
    for database in databases:
        logger.info(f"Processing database: {database[0]}")
        tables = get_table_metadata(database[0])

        if not tables:
            logger.warning(f"No tables found in database: {database[0]}")
            continue

        all_tables.extend(tables)

        for table in tables:
            sample_data = get_sample_data(database[0], table.schema, table.name)
            fields = get_field_metadata(database[0], table.oid, sample_data)

            all_table_fields[table.id] = fields

    save_table_metadata(all_tables, timestamp)
    save_field_metadata(all_table_fields, timestamp)

def main():
    collect_metadata()

if __name__ == "__main__":
    main()
