from datetime import timedelta
import glob
import sys
import os
import validation

sys.path.append('../')

import logging

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

@dag(
    dag_id='validate_data',
    description='Take files and validate the quality',
    tags=['dsp', 'validate'],
    schedule=timedelta(minutes=2),
    start_date=days_ago(n=0, hour=1)
)
def validate_data():
    @task
    def get_data_validated() -> str:
        default_folder = "dsp-finalproject/data/Folder A"
        good_folder = "dsp-finalproject/data/Folder C"
        failed_folder = "dsp-finalproject/data/Folder B"

        file_pattern = os.path.join(default_folder, "*.csv")
        file_paths = glob.glob(file_pattern)

        for file in file_paths:
            logging.info(f'{file}')
            validation.save_files_to_correct_folder(file, failed_folder, good_folder)

    get_data_validated()


validation_dag = validate_data()
