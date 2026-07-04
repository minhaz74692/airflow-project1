from airflow.sdk import asset, task
import pendulum 
import os
from asset_13_dag import fetch_data


@asset(
    schedule = fetch_data,  # This schedule expression schedules the asset to run daily
    name="process_data",
    description="Process the fetched data",
    
    # this is optional, but it is a good practice to specify the output file path for the asset
    uri = "/opt/airflow/logs/data/data_processed.txt",
)
def process_data(self): 

    # Create the directory if it doesn't exist
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)

    with open(self.uri, "w") as f:
        f.write(
            f"Data processed on {pendulum.now('Asia/Dhaka')}"
        ) 
    print(f"Data processed and written to {self.uri} on {pendulum.now('Asia/Dhaka')}")
