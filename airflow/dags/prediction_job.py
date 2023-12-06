import logging
from datetime import datetime
from datetime import timedelta

import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago


@dag(
    dag_id='prediction_job',
    description='Take files and output predictions',
    tags=['dsp', 'prediction_job'],
    schedule=timedelta(minutes=5),
    start_date=days_ago(n=0, hour=1)
)
def prediction_job():
    pass
