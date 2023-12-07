import logging
from datetime import datetime
from datetime import timedelta
from interface.utils import get_prediction
from interface.main import API_URL, GET_API_URL

import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import json


@dag(
    dag_id='prediction_job',
    description='Take files and output predictions',
    tags=['dsp', 'prediction_job'],
    schedule=timedelta(minutes=5),
    start_date=days_ago(n=0, hour=1)
)
def scheduled_job():
    """
    @task
    def get_data():
        pass
    """

    @task
    def make_predictions():
        uploaded_data = pd.read_csv("../data/Folder C/customers_file_1.csv")

        # Make predictions for each row in the uploaded CSV
        predictions = []

        for index, row in uploaded_data.iterrows():
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
                "SatisfactionScore": row["Satisfaction Score"],
                "CardType": row["Card Type"],
                "PointEarned": row["Point Earned"],
                "PredictionSource": "scheduled"
            }

            prediction_result = get_prediction(API_URL, prediction_data)
            predictions.append(prediction_result)


scheduled_job_dag = scheduled_job()




