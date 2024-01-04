from datetime import datetime
from datetime import timedelta
import sys
import os

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
def prediction_job():
    @task
    def read_csv_function():
        # Read the CSV file
        df = pd.read_csv("../dsp-finalproject/data/Folder C/test_file.csv")
        df["PredictionSource"] = "scheduled"

        data = df.to_dict(orient="records")
        logging.info(f'{data}')

        return data

    @task
    def make_predictions(data):
        """
        response = requests.post(
            API_URL,
            data=json.dumps(data),
            headers={"Content-Type": "application/json"},
        )

        response_data = response.json()
        prediction = response_data["prediction"]
        logging.info(f'{prediction}')
        """
        try:
            response = requests.get(API_URL)
            response.raise_for_status()

            print(f"API is reachable. Status code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to reach the API. Error: {e}")

    customer_data = read_csv_function()
    make_predictions(customer_data)


scheduled_job_dag = prediction_job()
