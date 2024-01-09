import streamlit as st
from utils import get_past_predictions, display_past_predictions
import datetime


# Create the past predictions page
def past_predictions_page(api_url, csv_file):
    st.subheader("View Past Predictions")
    start_date = st.date_input("Start Date")
    today = datetime.date.today()
    end_date = st.date_input("End Date",
                             max_value=today + datetime.timedelta(days=1))

    prediction_source = st.selectbox("Prediction Source", ["webpage", "scheduled", "all"])
    data = {
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "pred_source" : prediction_source
    }
    # Get past predictions from the CSV file
    predictions = get_past_predictions(api_url, data)

    # Display past predictions in a table or chart
    display_past_predictions(predictions)
