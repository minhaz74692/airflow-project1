from airflow.sdk import dag, task
from pendulum import datetime, duration
from datetime import timedelta
from airflow.timetables.trigger import DeltaTriggerTimetable

@dag(
    dag_id="schedule_delta_dag",
    start_date=datetime(year=2026, month=7, day=1, hour= 2, tz="Asia/Dhaka"), # This start date is set to 2 AM on July 1, 2026, in the Asia/Dhaka timezone
    schedule = DeltaTriggerTimetable(duration(days=5)),  # This delta expression schedules the DAG to run every 3 day
    # schedule=DeltaTriggerTimetable(delta=timedelta(days=3)),  # This delta expression schedules the DAG to run every 3 day
    is_paused_upon_creation=False,
    catchup=True, #catchup is enabled, so the DAG will run for all the past dates since the start_date
)
def schedule_delta_dag():

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
schedule_delta_dag()

