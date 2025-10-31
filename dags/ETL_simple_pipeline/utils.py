from pathlib import Path


# Define base dir for sql files
BASE_DIR = Path(__file__).parent.resolve()
SQL_DIR = BASE_DIR / 'sql_files'


# Helper fn to load sql files
def load_sql(file_name):
    with open(SQL_DIR / file_name, 'r') as f:
        return f.read()
