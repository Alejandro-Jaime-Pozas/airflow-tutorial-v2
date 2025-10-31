# Building a Simple Data Pipeline
# Welcome to the third tutorial in our series! At this point, you’ve already written your first Dag and used some basic operators. Now it’s time to build a small but meaningful data pipeline – one that retrieves data from an external source, loads it into a database, and cleans it up along the way.

# This tutorial introduces the SQLExecuteQueryOperator, a flexible and modern way to execute SQL in Airflow. We’ll use it to interact with a local Postgres database, which we’ll configure in the Airflow UI.

# By the end of this tutorial, you’ll have a working pipeline that:

# Downloads a CSV file

# Loads the data into a staging table

# Cleans the data and upserts it into a target table


from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from pathlib import Path

# Define sql file names
create_employees_table = 'create_employees_table.sql'
create_employees_temp_table = 'create_employees_temp_table.sql'

# Define base dir for sql files
BASE_DIR = Path(__file__).parent.resolve()
SQL_DIR = BASE_DIR / 'sql_files'

# Helper fn to load sql files
def load_sql(file_name):
    with open(SQL_DIR / file_name, 'r') as f:
        return f.read()

create_employees_table = SQLExecuteQueryOperator(
    task_id='create_employees_table',
    conn_id='tutorial_pg_connect',
    sql=load_sql(create_employees_table)
)

create_employees_temp_table = SQLExecuteQueryOperator(
    task_id='create_employees_temp_table',
    conn_id='tutorial_pg_connect',
    sql=load_sql(create_employees_temp_table)
)
