from datetime import datetime, timedelta
from airflow.decorators import dag, task
import requests
import os
import pandas as pd
import boto3
from io import StringIO, BytesIO

@dag(
    dag_id="ladot_parking_meter_ingestion_daily",
    start_date=datetime(2026, 6, 8), # set to a past date to allow immediate execution
    catchup=False, # don't backfill missed runs
    schedule=timedelta(days=1), # use timedelta for daily schedule precision
    default_args={
        "owner": "airflow",
        "depends_on_past": False, # if yesterday's run fails, it won't block today's run
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def ladot_parking_meter_ingestion_daily():
    """
    This is the main ingestion function. 
    We will call the individual tasks for each data source here.
    """
    @task(task_id="ingest_meter_occupancy_data")
    def extract_and_load_meter_occupancy_data():
        """
        We're taking our raw ingestion logic from pipeline.py and putting it here:
        """

        #1. set up variables to insert into request
        app_token = os.getenv("APP_TOKEN")
        s3_bucket = os.getenv("AWS_S3_BUCKET")
        
        current_date = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_name = f"meter_occupancy/run_{current_date}.parquet"

        headers = {
            "X-App-Token": app_token,
            "Content-Type": "application/json",
        }
        payload_csv = {
            "query": "SELECT *",
            "orderingSpecifier": "discard"
        }
        meter_occupancy_csv = "https://data.lacity.org/api/v3/views/e7h6-4a3e/export.csv"

        #2. retrieve data from API
        meter_occupancy_response = requests.post(meter_occupancy_csv, headers=headers, json=payload_csv, timeout=100)
        meter_occupancy_response.raise_for_status() # Check if the request was successful

        #3. convert to dataframe
        df = pd.read_csv(StringIO(meter_occupancy_response.text))

        #4. write to parquet in memory
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine="pyarrow", compression="snappy")

        #5. stream parquet file to S3
        s3_client = boto3.client("s3")
        try:
            response =  s3_client.put_object(
                Bucket=s3_bucket,
                Key=file_name,
                Body=parquet_buffer.getvalue()
            )

            print(f"Successfully uploaded {file_name} to S3 bucket {s3_bucket}")
        except Exception as e:
            print(f"Error uploading {file_name} to S3 bucket {s3_bucket}: {e}")
            raise
        
    # Instantiate the task(s)
    run_meter_occupancy_data = extract_and_load_meter_occupancy_data()

# Instantiate DAG
ladot_dag = ladot_parking_meter_ingestion_daily()