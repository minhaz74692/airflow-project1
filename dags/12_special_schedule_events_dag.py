from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.events import EventsTimetable

spacial_dates = EventsTimetable(
    event_dates=[
        datetime(year=2026, month=7, day=1, tz="Asia/Dhaka"),
        datetime(year=2026, month=7, day=5, tz="Asia/Dhaka"),
        datetime(year=2026, month=7, day=10, tz="Asia/Dhaka"),
    ]
)

@dag(
    dag_id="special_schedule_events_dag",
    schedule=spacial_dates,
    start_date=datetime(year=2026, month=7, day=1, tz="Asia/Dhaka"),
    end_date=datetime(year=2026, month=7, day=11, tz="Asia/Dhaka"),
    # is_paused_upon_creation=False,
    catchup=True, #catchup is enabled, so the DAG will run for all the past dates since the start_date
)
def special_schedule_events_dag():
    @task.python
    def special_event_task(**kwargs):
        execution_date = kwargs['logical_date']
        print(f"Special event task executed on {execution_date}")
    
    special_event_task()

#instantiate the DAG
special_schedule_events_dag()