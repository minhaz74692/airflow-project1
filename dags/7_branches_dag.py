from airflow.sdk import dag, task

@dag(
    dag_id="branches_dag",
)
def branches_dag():

    @task.python
    def extract_task(**kwargs):
        ti = kwargs['ti']
        print("Extracting data from kwargs ==> Task 1 executed")

        fetched_data = {
            "api_extracted_data": [1, 2, 3, 4, 5],
            "db_extracted_data": [6, 7, 8, 9, 10],
            "s3_extracted_data": [11, 12, 13, 14, 15],
            "weekend_flag": False
        }

        ti.xcom_push(key='return_result', value=fetched_data)

    @task.python
    def api_transform_task(**kwargs):
        ti = kwargs['ti']
        print("Transforming API Extracted data from kwargs ==> Task 2 executed")

        #pulling data from XCom using the task instance
        api_extracted_data = ti.xcom_pull(key='return_result', task_ids='extract_task')["api_extracted_data"]

        api_transformed_data = [x * 2 for x in api_extracted_data]
        api_transformed_data_dict = {"transformed_api_data": api_transformed_data}
        ti.xcom_push(key='return_result', value=api_transformed_data_dict)

    @task.python
    def db_transform_task(**kwargs):
        ti = kwargs['ti']
        print("Transforming DB Extracted data from kwargs ==> Task 2 executed")

        #pulling data from XCom using the task instance
        db_extracted_data = ti.xcom_pull(key='return_result', task_ids='extract_task')["db_extracted_data"]

        db_transformed_data = [x * 2 for x in db_extracted_data]
        db_transformed_data_dict = {"transformed_db_data": db_transformed_data}
        ti.xcom_push(key='return_result', value=db_transformed_data_dict)

    @task.python
    def s3_transform_task(**kwargs):
        ti = kwargs['ti']
        print("Transforming S3 Extracted data from kwargs ==> Task 2 executed")

        #pulling data from XCom using the task instance
        s3_extracted_data = ti.xcom_pull(key='return_result', task_ids='extract_task')["s3_extracted_data"]

        s3_transformed_data = [x * 2 for x in s3_extracted_data]
        s3_transformed_data_dict = {"transformed_s3_data": s3_transformed_data}
        ti.xcom_push(key='return_result', value=s3_transformed_data_dict)

    @task.bash
    def load_task(**kwargs):
        ti = kwargs['ti']
        print("Loading data from kwargs ==> Task 3 executed")
        api_transformed_data = ti.xcom_pull(key='return_result', task_ids='api_transform_task')
        db_transformed_data = ti.xcom_pull(key='return_result', task_ids='db_transform_task')
        s3_transformed_data = ti.xcom_pull(key='return_result', task_ids='s3_transform_task')

        return f"echo 'Final Load Data: {api_transformed_data}, {db_transformed_data}, {s3_transformed_data}'"
    
    @task.bash
    def no_load_task(**kwargs):
        print("No Load Task Executed ==> Task 4 executed")
        return 'echo "No Load Task Executed: It\'s a weekend, skipping load."'


    ## Creating the decider branch
    @task.branch
    def decider_task(**kwargs):
        ti = kwargs['ti']
        weekend_flag = ti.xcom_pull(key='return_result', task_ids='extract_task')["weekend_flag"]

        if weekend_flag:
            return "no_load_task"
        else:
            return "load_task"

    # Define task dependencies
    extract = extract_task()
    api_transform = api_transform_task()
    db_transform = db_transform_task()
    s3_transform = s3_transform_task()
    load = load_task()
    no_load = no_load_task()
    decider = decider_task()

    extract >> [api_transform, db_transform, s3_transform] >> decider >> [load, no_load]


#instantiate the DAG
branches_dag()

