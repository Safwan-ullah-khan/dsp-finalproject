import streamlit as st
from utils import load_data, get_past_predictions, display_past_predictions
import datetime


# Create the past predictions page
def past_predictions_page(csv_file):
    st.subheader("View Past Predictions")
    start_date = st.date_input("Start Date")
    today = datetime.date.today()
    end_date = st.date_input("End Date", max_value=today)

    prediction_source = st.selectbox("Prediction Source", ["webapp", "scheduled predictions", "all"])

    # Get past predictions from the CSV file
    predictions = get_past_predictions(start_date, end_date, prediction_source)

    # Display past predictions in a table or chart
    display_past_predictions(predictions)
