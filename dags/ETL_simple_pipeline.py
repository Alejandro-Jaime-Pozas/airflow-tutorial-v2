# Building a Simple Data Pipeline
# Welcome to the third tutorial in our series! At this point, you’ve already written your first Dag and used some basic operators. Now it’s time to build a small but meaningful data pipeline – one that retrieves data from an external source, loads it into a database, and cleans it up along the way.

# This tutorial introduces the SQLExecuteQueryOperator, a flexible and modern way to execute SQL in Airflow. We’ll use it to interact with a local Postgres database, which we’ll configure in the Airflow UI.

# By the end of this tutorial, you’ll have a working pipeline that:

# Downloads a CSV file

# Loads the data into a staging table

# Cleans the data and upserts it into a target table


