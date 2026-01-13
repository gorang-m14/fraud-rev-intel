import os
import psycopg2
from psycopg2.extras import execute_values

def pg_conn():
    dsn = os.environ.get("FRI_PG_DSN", "postgresql://fri:fri@localhost:5432/fri_oltp")
    return psycopg2.connect(dsn)

def insert_many(cur, sql, rows, page_size=1000):
    if not rows:
        return
    execute_values(cur, sql, rows, page_size=page_size)
