from airflow.sdk import asset, task
import pendulum 
import os


@asset(
    name="fetch_data",
    description="Fetch data from the source",
    schedule = "@daily",  # This schedule expression schedules the asset to run daily
    # this is optional, but it is a good practice to specify the output file path for the asset
    uri = "/opt/airflow/logs/data/data_extract.txt",
)
def fetch_data(self): 

    # Create the directory if it doesn't exist
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)

    with open(self.uri, "w") as f:
        f.write(
            f"Data fetched from the source on {pendulum.now('Asia/Dhaka')}"
        ) 
    print(f"Data fetched and written to {self.uri} on {pendulum.now('Asia/Dhaka')}")
