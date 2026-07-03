from airflow.sdk import dag, task

@dag(
    dag_id="first_dag",
)
def first_dag():

    @task.python
    def task_1():
        print("Task 1 executed")

    @task.python
    def task_2():
        print("Task 2 executed sdkfj")

    @task.python
    def task_3():
        print("Task 3 executed")
    
    @task.python
    def task_4():
        print("Task 4 executed")

    # Define task dependencies
    first = task_1()
    second = task_2()
    third = task_3()
    fourth = task_4()

    
    first >> second >> third >> fourth

#instantiate the DAG
first_dag()

