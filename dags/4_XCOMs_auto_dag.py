from airflow.sdk import dag, task

@dag(
    dag_id="xcoms_auto_dag",
)
def xcoms_auto_dag():

    @task.python
    def extract():
        print("Extracting data from source ==> Task 1 executed")
        fetched_data = {"data": [1, 2, 3, 4, 5]}
        return fetched_data

    @task.python
    def transform(data: dict):
        print("Transforming data ==> Task 2 executed")
        fetched_data = data["data"]
        transformed_data = [x * 2 for x in fetched_data]
        transformed_data_dict = {"transformed_data": transformed_data}
        return transformed_data_dict

    @task.python
    def load(data: dict):
        print("Loading data into destination ==> Task 3 executed")
        load_data = data
        return load_data
    

    # Define task dependencies
    first = extract()
    second = transform(first)
    third = load(second)


#instantiate the DAG
xcoms_auto_dag()

