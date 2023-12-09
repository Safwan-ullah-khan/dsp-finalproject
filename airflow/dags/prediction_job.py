from datetime import datetime
from datetime import timedelta
import sys

import requests

sys.path.append('../')

import pandas as pd
import logging
import json
import requests

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

API_URL = "http://127.0.0.1:8050/predict"


@dag(
    dag_id='prediction_job',
    description='Take files and output predictions',
    tags=['dsp', 'prediction_job'],
    schedule=timedelta(minutes=2),
    start_date=days_ago(n=0, hour=1)
)
def scheduled_job():
    @task
    def read_csv_function():
        # Read the CSV file
        df = pd.read_csv("../dsp-finalproject/data/Folder C/test_file.csv")

        # Get the customer data
        data = df.to_dict(orient="records")
        logging.info(f'{data}')
        # Return the customer data
        return data

    @task
    def process_response_function(data):
        # Call the API endpoint
        response = requests.post(
            API_URL,
            data=json.dumps(data),
            headers={"Content-Type": "application/json"},
        )

        # Process the response
        response_data = response.json()
        prediction = response_data["prediction"]
        logging.info(f'{prediction}')

    customer_data = read_csv_function()
    process_response_function(customer_data)


scheduled_job_dag = scheduled_job()
