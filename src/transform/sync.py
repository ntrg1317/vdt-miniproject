import logging, json, os
from datetime import datetime

import psycopg2
from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

POSTGRES_TARGET_HOST=os.getenv('POSTGRES_TARGET_HOST')
POSTGRES_TARGET_PORT=os.getenv('POSTGRES_TARGET_PORT')
POSTGRES_TARGET_USER=os.getenv('POSTGRES_TARGET_USER')
POSTGRES_TARGET_PASSWORD=os.getenv('POSTGRES_TARGET_PASSWORD')
POSTGRES_TARGET_DB=os.getenv('POSTGRES_TARGET_DB')

class Syncer:
    def __init__(self):
        self.kafka_config = {
            'bootstrap_servers': ['localhost:9092'],
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': True,
            'group_id': 'sync-pg-tables',
            'value_deserializer': lambda value: json.loads(value.decode('utf-8')),
        }

        self.postgres_target_config = {
            'host': 'localhost',
            'port': '5433',
            'user': 'debezium',
            'password': 'debezium',
            'database': 'debezium'
        }

        self.consumer = None
        self.pg_target_conn = None

    def connect_kafka(self):
        topic = "sourcepg.vdt.users"
        try:
            self.consumer = KafkaConsumer(topic, **self.kafka_config)
            logger.info(f'Connected to Kafka Topic: {topic}')
        except Exception as e:
            logger.error(f'Failed to connect to Kafka Topic: {topic}: {e}')

    def connect_postgres(self):
        try:
            self.pg_target_conn = psycopg2.connect(**self.postgres_target_config)
            self.pg_target_conn.autocommit = True
            logger.info("Connected to PostgreSQL")
        except Exception as e:
            logger.error(f'Failed to connect to Postgres Target: {e}')

    def consuming(self):
        try:
            for message in self.consumer:
                self.process_change_event(message)
        except KeyboardInterrupt:
            logger.info("Stop consumer")
        except Exception as e:
            logger.error(f"Error consuming: {e}")
        finally:
            self.cleanup()

    def process_change_event(self, message):
        if not message.value:
            return

        payload = message.value.get('payload')
        operation = payload.get('op')
        table_name = message.topic.split('.')[-1]

        logger.info(f'Processing change event: {operation} {table_name}')
        if operation == 'c' or operation == 'u':
            self.handle_insert_or_update(table_name, payload.get('after'))
        elif operation == 'd':
            self.handle_delete(table_name, payload.get('before'))
        elif operation == 'r':
            self.handle_insert_or_update(table_name, payload.get('after'))

    def handle_insert_or_update(self, table_name, data):
        if not data:
            return

        cursor = self.pg_target_conn.cursor()
        target_table = f"vdt_rep.{table_name}"

        first_name = data.pop("first_name", "")
        last_name = data.pop("last_name", "")
        full_name = first_name + ' ' + last_name

        data['full_name'] = full_name
        data['sync_at'] = datetime.now()
        data['created_at'] = datetime.fromtimestamp(data['created_at'] / 1000000)
        data['updated_at'] = datetime.fromtimestamp(data['updated_at'] / 1000000)

        columns = list(data.keys())
        values = list(data.values())

        insert_query = f"""
            INSERT INTO {target_table} ({', '.join(columns)})
            VALUES ({', '.join(['%s'] * len(columns))})
            ON CONFLICT (id) DO UPDATE SET
            {','.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'id'])}
        """

        cursor.execute(insert_query, values)
        logger.info(f"Insert/Update into {target_table}: {data}")

    def handle_delete(self, table_name, data):
        if not data:
            return

        cursor = self.pg_target_conn.cursor()
        target_table = f"vdt_rep.{table_name}"

        delete_query = f"""
        DELETE FROM {target_table} WHERE id = %s
        """

        cursor.execute(delete_query, (data['id'],))
        logger.info(f"Successfully delete from {target_table}: {data}")

    def cleanup(self):
        if self.pg_target_conn:
            self.pg_target_conn.close()
        if self.consumer:
            self.consumer.close()



def main():
    logger.info("Starting")
    syncer = Syncer()

    logger.info('Connecting Kafka')
    syncer.connect_kafka()

    logger.info('Connecting PostgreSQL')
    syncer.connect_postgres()

    logger.info('Starting consumer')
    syncer.consuming()

if __name__ == "__main__":
    main()