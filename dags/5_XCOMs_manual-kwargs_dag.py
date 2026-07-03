from airflow.sdk import dag, task

@dag(
    dag_id="xcoms_manaual_kwargs_dag",
)
def xcoms_manaual_kwargs_dag():

    @task.python
    def extract(**kwargs):
        #Extracting 'ti': (task instance) from kwargs to use it for pushing data to XCom
        ti = kwargs['ti']
        print("Extracting data from kwargs ==> Task 1 executed")

        fetched_data = {"data": [1, 2, 3, 4, 5]}
        #pushing data to XCom using the task instance
        ti.xcom_push(key='return_result', value=fetched_data)

    @task.python
    def transform(**kwargs):
        #Extracting 'ti': (task instance) from kwargs to use it for pulling data from XCom
        ti = kwargs['ti']
        print("Transforming data from kwargs ==> Task 2 executed")

        #pulling data from XCom using the task instance
        fetched_data = ti.xcom_pull(task_ids='extract')

        transformed_data = [x * 2 for x in fetched_data["data"]]
        transformed_data_dict = {"transformed_data": transformed_data}
        ti.xcom_push(key='return_result', value=transformed_data_dict)

    @task.python
    def load(**kwargs):
        ti = kwargs['ti']
        print("Loading data from kwargs ==> Task 3 executed")
        load_data = ti.xcom_pull(task_ids='transform')
        
        ti.xcom_push(key='return_result', value=load_data)
    

    # Define task dependencies
    first = extract()
    second = transform()
    third = load()

    first >> second >> third


#instantiate the DAG
xcoms_manaual_kwargs_dag()

