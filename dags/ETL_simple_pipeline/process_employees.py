# Contains all other py files put together into a single dag

import datetime, pendulum
import os
import requests

from ETL_simple_pipeline.utils import BASE_DIR, SQL_DIR, load_sql

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook  # this needs to be pip installed to access
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# This has the consolidated code from the other py files
@dag(
    dag_id='process_employees',
    description='Dag that processes employees data',
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessEmployees():

    # Define sql file names
    create_employees_table_file = 'create_employees_table.sql'
    create_employees_temp_table_file = 'create_employees_temp_table.sql'
    merge_into_employees_file = 'merge_into_employees.sql'

    # Create the main employees table
    create_employees_table = SQLExecuteQueryOperator(
        task_id='create_employees_table',
        conn_id='tutorial_pg_connect',
        sql=load_sql(SQL_DIR / create_employees_table_file)
    )

    # Create the stage employees table
    create_employees_temp_table = SQLExecuteQueryOperator(
        task_id='create_employees_temp_table',
        conn_id='tutorial_pg_connect',
        sql=load_sql(SQL_DIR / create_employees_temp_table_file)
    )

    # Next, we’ll download a CSV file, save it locally, and load it into employees_temp using the PostgresHook.
    @task
    def copy_data_to_postgres():
        # NOTE: configure as needed for your airflow env

        # Set the empty csv file path for docker container
        data_path = '/opt/airflow/dags/files/employees.csv'
        os.makedirs(os.path.dirname(data_path), exist_ok=True)

        # Get the file example
        url = "https://raw.githubusercontent.com/apache/airflow/main/airflow-core/docs/tutorial/pipeline_example.csv"
        response = requests.get(url)

        # write the file example to docker env csv file
        with open(data_path, 'w') as f:
            f.write(response.text)

        # connect to postgres
        postgres_hook = PostgresHook(postgres_conn_id='tutorial_pg_connect')
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        # copy the csv file into postgres
        with open(data_path, 'r') as f:
            cur.copy_expert(
                "COPY employees_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                f
            )
        conn.commit()

    @task
    def merge_data():
        query = load_sql(SQL_DIR / merge_into_employees_file)
        try:
            postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_connect")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            return 0
        except Exception as e:
            return 1

    [create_employees_table, create_employees_temp_table] >> copy_data_to_postgres() >> merge_data()

dag = ProcessEmployees()
