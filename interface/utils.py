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


def get_past_predictions(api_url, data):

    response = requests.get(api_url, json=data)

    #if response.status_code == 200:
        # Display the results returned by the API
       # past_predictions = response.json()
    print(response.content)
    prediction = pd.DataFrame(response.json())

    return prediction


def display_past_predictions(predictions):
    #if predictions.empty:
     #   st.write("No past predictions found for the selected date range and source.")
    #else:
    st.write("Past Predictions:")
    st.table(predictions)
