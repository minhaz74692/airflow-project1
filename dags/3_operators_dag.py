from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator

@dag(
    dag_id="operators_dag",
)
def operators_dag():

    @task.python
    def task_1():
        print("Task 1 executed")

    @task.python
    def task_2():
        print("Task 2 executed sdkfj")

    @task.python
    def task_3():
        print("Task 3 executed")
    
    @task.bash
    def bash_task_modern() -> str:
        return "echo https://airflow.apache.org/"

    bash_task_oldschool = BashOperator(
        task_id="run_after_loop",
        bash_command="echo https://airflow.apache.org/",
    )
    

    # Define task dependencies
    first = task_1()
    second = task_2()
    third = task_3()
    bash_task_modern = bash_task_modern()

    
    first >> second >> third >> bash_task_modern >> bash_task_oldschool

#instantiate the DAG
operators_dag()

