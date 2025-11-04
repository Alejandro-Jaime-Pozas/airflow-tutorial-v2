import json
import pendulum

from airflow.sdk import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def tutorial_taskflow_api():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """
    @task(
        retries=3,
    )
    def extract():
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.
        """
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'

        order_data_dict = json.loads(data_string)
        return order_data_dict

    @task(multiple_outputs=True)
    def transform(order_data_dict: dict):
        """
        #### Transform task
        A simple Transform task which takes in the collection of order data and
        computes the total order value.
        """
        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}

    @task(
        retries=3,
    )
    def load(total_order_value: float):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """

        print('Hello World')
        print(f"Total order value is: {total_order_value:.2f}")
        return 'Successful load'


    # Example of using the virtualenv decorator to run a task with specific code
    @task.virtualenv(
        task_id="virtualenv_python", requirements=["colorama==0.4.0"], system_site_packages=False
    )
    def callable_virtualenv():
        """
        Example function that will be performed in a virtual environment.

        Importing at the module level ensures that it will not attempt to import the
        library before it is installed.
        """
        from time import sleep

        from colorama import Back, Fore, Style

        print(Fore.RED + "some red text")
        print(Back.GREEN + "and with a green background")
        print(Style.DIM + "and in dim text")
        print(Style.RESET_ALL)
        for _ in range(4):
            print(Style.DIM + "Please wait...", flush=True)
            sleep(1)
        print("Finished")

    # # Example of using the external_python decorator to run a task with a specific python binary
    # @task.external_python(task_id="external_python", python=PATH_TO_PYTHON_BINARY)
    # def callable_external_python():
    #     """
    #     Example function that will be performed in a virtual environment.

    #     Importing at the module level ensures that it will not attempt to import the
    #     library before it is installed.
    #     """
    #     import sys
    #     from time import sleep

    #     print(f"Running task via {sys.executable}")
    #     print("Sleeping")
    #     for _ in range(4):
    #         print("Please wait...", flush=True)
    #         sleep(1)
    #     print("Finished")

    # external_python_task = callable_external_python()


    # # Example of using the docker decorator to run a task within a docker container.
    # # Runs your task in a Docker container. Useful for packaging everything the task needs
    # # — but requires Docker to be available on your worker.
    # @task.docker(image="python:3.9-slim-bookworm", multiple_outputs=True)
    # def transform(order_data_dict: dict):
    #     """
    #     #### Transform task
    #     A simple Transform task which takes in the collection of order data and
    #     computes the total order value.
    #     """
    #     total_order_value = 0

    #     for value in order_data_dict.values():
    #         total_order_value += value

    #     return {"total_order_value": total_order_value}


    # # Runs your task inside a Kubernetes pod, fully isolated from the main Airflow environment.
    # # Ideal for large tasks or tasks requiring custom runtimes.
    # @task.kubernetes(
    #     image="python:3.9-slim-buster",
    #     name="k8s_test",
    #     namespace="default",
    #     in_cluster=False,
    #     config_file="/path/to/.kube/config",
    # )
    # def execute_in_k8s_pod():
    #     import time

    #     print("Hello from k8s pod")
    #     time.sleep(2)

    # @task.kubernetes(image="python:3.9-slim-buster", namespace="default", in_cluster=False)
    # def print_pattern():
    #     n = 5
    #     for i in range(n):
    #         # inner loop to handle number of columns
    #         # values changing acc. to outer loop
    #         for _ in range(i + 1):
    #             # printing stars
    #             print("* ", end="")

    #         # ending line after each row
    #         print("\r")

    # execute_in_k8s_pod_instance = execute_in_k8s_pod()
    # print_pattern_instance = print_pattern()

    # execute_in_k8s_pod_instance >> print_pattern_instance


    virtualenv_task = callable_virtualenv()

    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"])

tutorial_taskflow_api()
