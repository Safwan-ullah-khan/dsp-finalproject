import pandas as pd
import requests
import streamlit as st


def load_data(file):
    df = pd.read_csv(file)
    return df


def get_prediction(api_url, data):
    response = requests.post(api_url, json=data)
    prediction = response.json()
    return prediction


def display_prediction(data, prediction):
    df = data.copy()
    df["Prediction"] = prediction
    st.table(df)


def get_past_predictions(file, start_date, end_date, source):
    df = load_data(file)
    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    df = df[(df["Date"] >= start_date) & (df["Date"] <= end_date)]
    if source != "all":
        df = df[df["Source"] == source]
    return df


def display_past_predictions(predictions):
    if predictions.empty:
        st.write("No past predictions found for the selected date range and source.")
    else:
        st.write("Past Predictions:")
        st.table(predictions)
