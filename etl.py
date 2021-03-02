import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    print('Loading staging tables!!')
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    print('Inserting into tables!!')
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main_etl():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    keys = {
        'host': config.get('DWH', 'DWH_ENDPOINT'),
        'database': config.get('DWH', 'DWH_DB'),
        'username': config.get('DWH', 'DWH_DB_USER'),
        'password': config.get('DWH', 'DWH_DB_PASSWORD'),
        'port' : config.get('DWH', 'DWH_PORT')
    }
    conn_string = f"postgresql://{keys['username']}:{keys['password']}@{keys['host']}:{keys['port']}/{keys['database']}"
    conn = psycopg2.connect(
        conn_string
    )
    cur = conn.cursor()

    load_staging_tables(cur, conn)

    insert_tables(cur, conn)

    conn.close()
