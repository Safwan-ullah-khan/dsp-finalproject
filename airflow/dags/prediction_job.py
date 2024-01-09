from datetime import datetime
from datetime import timedelta
import sys
import os

sys.path.append('../')

import pandas as pd
import logging
import json
import requests

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

API_URL = "http://host.docker.internal:8050/predict/"


@dag(
    dag_id='prediction_job',
    description='Take files and output predictions',
    tags=['dsp', 'prediction_job'],
    schedule=timedelta(minutes=2),
    start_date=days_ago(n=0, hour=1)
)
def prediction_job():
    """
    @task
    def read_csv_function():
        # Read the CSV file
        df = pd.read_csv("/opt/data/Folder C/test_file2.csv")
        df["PredictionSource"] = "scheduled"

        data = df.to_dict(orient="records")
        logging.info(f'{type(data)}')
        return data
    """

    @task
    def make_predictions():
        df = pd.read_csv("/opt/data/Folder C/test_file2.csv")
        for _, row in df.iterrows():
            prediction_data = {
                "CreditScore": row["CreditScore"],
                "Gender": row["Gender"],
                "Age": row["Age"],
                "Tenure": row["Tenure"],
                "Balance": row["Balance"],
                "NumOfProducts": row["NumOfProducts"],
                "HasCrCard": row["HasCrCard"],
                "IsActiveMember": row["IsActiveMember"],
                "EstimatedSalary": row["EstimatedSalary"],
                "SatisfactionScore": row["SatisfactionScore"],
                "CardType": row["CardType"],
                "PointEarned": row["PointEarned"],
                "PredictionSource": "scheduled"
            }

        response = requests.post(
            API_URL,
            json=prediction_data
        )

        response_data = response.json()
        prediction = response_data["prediction"]
        logging.info(f'{prediction}')

    #customer_data = read_csv_function()
    make_predictions()


scheduled_job_dag = prediction_job()
