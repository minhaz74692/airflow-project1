from airflow.sdk import dag, task
from pendulum import datetime, duration
from airflow.timetables.interval import CronDataIntervalTimetable

@dag(
    dag_id="incremental_load_dag",
    start_date=datetime(year=2026, month=7, day=1, tz="Asia/Dhaka"),
    end_date=datetime(year=2026, month=7, day=10, tz="Asia/Dhaka"),
    schedule=CronDataIntervalTimetable("@daily", timezone="Asia/Dhaka"),  # This cron expression schedules the DAG to run at 2 AM every day
    # is_paused_upon_creation=False,
    catchup=True, #catchup is enabled, so the DAG will run for all the past dates since the start_date
)
def incremental_load_dag():

    @task.python
    def incremental_data_fetch(**kwargs):
        date_interval_start = kwargs['data_interval_start']
        date_interval_end = kwargs['data_interval_end']
        print(f"Incremental data fetch from {date_interval_start} to {date_interval_end}")

    @task.bash
    def incremental_data_process():
        print("Incremental data processing")
        return "echo 'Processing incremental data from {{data_interval_start}} to {{data_interval_end}}'"

    # Define task dependencies
    fetch_task = incremental_data_fetch()
    process_task = incremental_data_process()

    fetch_task >> process_task

#instantiate the DAG
incremental_load_dag()