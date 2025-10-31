from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from pathlib import Path
from utils import SQL_DIR, load_sql


merge_into_employees = 'merge_into_employees.sql'

@task
def merge_data():
    query = load_sql(merge_into_employees)
    try:
        postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_connect")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        return 0
    except Exception as e:
        return 1
