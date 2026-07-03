from airflow.sdk import dag, task

@dag(
    dag_id="versioned_dag",
)
def versioned_dag():

    @task.python
    def task_1():
        print("Task 1 executed")

    @task.python
    def task_2():
        print("Task 2 executed")

    @task.python
    def task_3():
        print("Task 3 executed")
    
    @task.python
    def task_4():
        print("Task 4 executed")

    
    @task.python
    def task_5():
        print("Task 5 executed: This is a new task added in the versioned DAG.")

    # Define task dependencies
    first = task_1()
    second = task_2()
    third = task_3()
    fourth = task_4()
    fifth = task_5()

    first >> second >> third >> fourth >> fifth

#instantiate the DAG
versioned_dag()

