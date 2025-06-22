import logging, json, os
from datetime import datetime

import pandas as pd
from kafka import KafkaConsumer
from sqlalchemy import create_engine
from config.settings import settings
from src.service.chart import ChartService

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Syncer:
    def __init__(self):
        self.kafka_config = {
            'bootstrap_servers': ['localhost:9092'],
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': True,
            'group_id': 'sync-pg-tables',
            'value_deserializer': lambda value: json.loads(value.decode('utf-8')),
        }

        self.consumer = None
        self.engine = None

    def connect_kafka(self):
        try:
            self.consumer = KafkaConsumer(**self.kafka_config)
            self.consumer.subscribe(pattern='^sourcepg\..*')
        except Exception as e:
            logger.error(f'Failed to connect to Kafka: {e}')

    def connect_postgres(self):
        try:
            self.engine = create_engine(settings.target_database_url)
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
            self.handle_update(payload.get('after'))
        # elif operation == 'd':
        #     self.handle_delete(table_name, payload.get('before'))
        # elif operation == 'r':
        #     self.handle_insert_or_update(table_name, payload.get('after'))

    def handle_insert_or_update(self, table_name, data):
        if not data:
            return

        # conn = self.engine.connect()
        # target_table = f"catalog.{table_name}"
        #
        # columns = list(data.keys())
        # values = list(data.values())
        #
        # insert_query = f"""
        #     INSERT INTO {target_table} ({', '.join(columns)})
        #     VALUES ({', '.join(['%s'] * len(columns))})
        #     ON CONFLICT (id) DO UPDATE SET
        #     {','.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'id'])}
        # """
        #
        # logger.info(f"Insert/Update into {target_table}: {data}")

    def handle_update(self, data):
        if not data:
            return
        engine = self.engine

        row_id = data['hash_id']

        chart = ChartService(engine)
        chart_data = chart.get_data_row_id(row_id)
        chart_type = chart_data['type']

        if chart_type == 'dial':
            title = chart_data['title']
            config = {
                'value_column': data['tt5'],
                'threshold_column': data['tt4'],
            }
            filters = chart_data.get('filters') or {}
            single_row_data = pd.DataFrame([chart_data])
            res = chart.create_chart(single_row_data, 'dial', title, config, filters)
            chart.save_chart(chart_data['dashboard_id'], row_id, data["ind_name"], title,
                                     'dial', res['json_data'], res['config'], res['filters'])

    # def handle_delete(self, table_name, data):
    #     if not data:
    #         return
    #
    #     cursor = self.pg_target_conn.cursor()
    #     target_table = f"vdt_rep.{table_name}"
    #
    #     delete_query = f"""
    #     DELETE FROM {target_table} WHERE id = %s
    #     """
    #
    #     cursor.execute(delete_query, (data['id'],))
    #     logger.info(f"Successfully delete from {target_table}: {data}")
    #
    def cleanup(self):
        if self.engine:
            self.engine.dispose()
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