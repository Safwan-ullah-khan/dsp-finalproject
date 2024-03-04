from typing import List
import pandas as pd
import requests
import streamlit as st


def load_data(file: str) -> pd.DataFrame:
    """ Return a pandas DataFrame from a csv file.

    Argument:
    file: CSV file path
    """
    df = pd.read_csv(file)
    return df


def get_prediction(api_url: str, data: List[dict]) -> int:
    """ Return the prediction from the API.

    Arguments:
    api_url: API URL
    data: Data for prediction
    """
    response = requests.post(api_url, json=data)
    prediction = response.json()
    prediction_value = prediction['prediction'][0]
    return prediction_value["PredictionResult"]


def display_prediction(data: dict, prediction: List[int]) -> None:
    """ Display the prediction result in a table.

    Arguments:
    data: Data for prediction
    prediction: Prediction result
    """
    df = data.copy()
    df["Prediction"] = prediction
    st.table(df)


def get_past_predictions(api_url: str, data: dict) -> pd.DataFrame:
    """ Return past predictions from the API.

    Arguments:
    api_url: API URL
    data: Data for past predictions
    """
    response = requests.get(api_url, json=data)
    # print(response.content)
    prediction = pd.DataFrame(response.json())
    return prediction


def display_past_predictions(predictions: pd.DataFrame) -> None:
    """ Display past predictions in a table.

    Argument:
    predictions: Past predictions from API
    """
    st.write("Past Predictions:")
    st.table(predictions)
