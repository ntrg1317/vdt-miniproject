import os

import psycopg2
from psycopg2 import sql, OperationalError

SRC_HOST = os.getenv('POSTGRES_SOURCE_HOST')
SRC_PORT = os.getenv('POSTGRES_SOURCE_PORT')
SRC_USER = os.getenv('POSTGRES_SOURCE_USER')
SRC_PASSWORD = os.getenv('POSTGRES_SOURCE_PASSWORD')

TARG_HOST = os.getenv('POSTGRES_TARGET_HOST')
TARG_PORT = os.getenv('POSTGRES_TARGET_PORT')
TARG_USER = os.getenv('POSTGRES_TARGET_USER')
TARG_PASSWORD = os.getenv('POSTGRES_TARGET_PASSWORD')

class PostgresConn:
    def __init__(self, dbtype, **kwargs):
        self.datasource = "PostgreSQL"
        if dbtype == "source":
            self.host = SRC_HOST
            self.port = SRC_PORT
            self.username = SRC_USER
            self.password = SRC_PASSWORD
        elif dbtype == "target":
            self.host = TARG_HOST
            self.port = TARG_PORT
            self.username = TARG_USER
            self.password = TARG_PASSWORD
        else:
            self.host = kwargs.get('host')
            self.port = kwargs.get('port')
            self.username = kwargs.get('user')
            self.password = kwargs.get('password')

        self.db = kwargs.get('db', 'postgres')
        self.conn = None
        self.connect()

    def __del__(self):
        self.close()

    def get_conn(self):
        return self.conn

    def get_datasource(self):
        return self.datasource

    def close(self):
        if self.conn:
            self.conn.close()

    def connect(self):
        print(f"Connecting to {self.host}:{self.port} as {self.username}")
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            dbname=self.db
        )

    def select(self, query, params=None):
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, params)
            return cursor.fetchall()
        except OperationalError as e:
            print(f"[ERROR] OperationalError: {e}")
            raise
        finally:
            cursor.close()

    def truncate(self, tablename):
        cursor = self.conn.cursor()
        try:
            cursor.execute("TRUNCATE TABLE {} RESTART IDENTITY;".format(tablename))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"[ERROR] Truncate failed: {e}")
            raise
        finally:
            cursor.close()

    def insert(self, query, params=None):
        with self.conn.cursor() as cursor:
            try:
                cursor.execute(query, params)
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                print(f"[ERROR] Insert failed: {e}")
                raise

    def batch_insert(self, query, data):
        try:
            cursor = self.conn.cursor()
            cursor.executemany(query, data)
            self.conn.commit()
            cursor.close()
        except Exception as e:
            self.conn.rollback()
            print(f"[ERROR] Insert failed: {e}")
            raise

    def execute(self, query, params=None):
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, params)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"[ERROR] Execute failed: {e}")
            raise