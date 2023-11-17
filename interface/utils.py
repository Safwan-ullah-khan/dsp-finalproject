import pandas as pd
import requests
import streamlit as st


def load_data(file):
    df = pd.read_csv(file)
    return df


def get_prediction(api_url, data):
    response = requests.post(api_url, json=data)
    prediction = response.json()
    prediction_value = prediction['prediction'][0]
    return prediction_value


def display_prediction(data, prediction):
    df = data.copy()
    df["Prediction"] = prediction
    st.table(df)


def get_past_predictions(api_url, start_date, end_date, source):
    filtered_df = pd.DataFrame()
    response = requests.get(api_url)

    if response.status_code == 200:
        # Display the results returned by the API
        past_predictions = response.json()

        columns_list = ["PredictionId", "PredictionResult", "PredictionDate"]

        result_df = pd.DataFrame(past_predictions, columns=columns_list)
        result_df["PredictionDate"] = pd.to_datetime(result_df["PredictionDate"])
        result_df["PredictionDate"] = result_df["PredictionDate"].dt.date
        filtered_df = result_df[(result_df['PredictionDate'] >= start_date) & (
                    result_df['PredictionDate'] < end_date)]

    return filtered_df


def display_past_predictions(predictions):
    if predictions.empty:
        st.write("No past predictions found for the selected date range and source.")
    else:
        st.write("Past Predictions:")
        st.table(predictions)
