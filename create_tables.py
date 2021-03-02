import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    print('Dropping tables if they do exist!!')
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    print('Creating tables!!')
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main_tables_create():
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

    drop_tables(cur, conn)

    create_tables(cur, conn)

    conn.close()


