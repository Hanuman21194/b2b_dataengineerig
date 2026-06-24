try:
    import psycopg2 as pg_driver
except ImportError:
    import psycopg as pg_driver

from config import DB_CONFIG


def get_connection(autocommit=False):
    conn = pg_driver.connect(**DB_CONFIG)
    conn.autocommit = autocommit
    return conn


def fetch_one_value(cursor, sql, params=None, default=None):
    cursor.execute(sql, params or ())
    row = cursor.fetchone()
    if not row:
        return default
    return row[0]
