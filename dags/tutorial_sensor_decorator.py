# Sensors wait for external conditions before proceeding.

# They’re a lightweight way to model dependencies outside Airflow.

# @task.sensor makes it simple to define them as Python functions.

# They support both poke (blocking) and reschedule (non-blocking) modes.

# The function returns a PokeReturnValue, telling Airflow when it’s done and optionally passing data via XCom.


import pendulum

from airflow.sdk import PokeReturnValue, dag, task
@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def example_sensor_decorator():
    # Using a sensor operator to wait for the upstream data to be ready.
    @task.sensor(poke_interval=60, timeout=3600, mode="reschedule")
    def wait_for_upstream() -> PokeReturnValue:
        return PokeReturnValue(is_done=True, xcom_value="xcom_value")
    @task
    def dummy_operator() -> None:
        pass
    wait_for_upstream() >> dummy_operator()
tutorial_etl_dag = example_sensor_decorator()
