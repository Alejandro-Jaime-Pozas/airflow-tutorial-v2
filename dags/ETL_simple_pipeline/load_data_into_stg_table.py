import os
import requests
from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

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
