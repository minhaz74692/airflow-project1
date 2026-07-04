from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.trigger import CronTriggerTimetable

@dag(
    dag_id="schedule_cron_job_dag",
    start_date=datetime(year=2026, month=7, day=1, tz="Asia/Dhaka"),
    schedule=CronTriggerTimetable("5 4 * * SUN-THU", timezone="Asia/Dhaka"),  # This cron expression schedules the DAG to run at 2:30 AM from Sunday to Thursday
    is_paused_upon_creation=False,
    catchup=True, #catchup is enabled, so the DAG will run for all the past dates since the start_date
)
def schedule_cron_job_dag():

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
schedule_cron_job_dag()

