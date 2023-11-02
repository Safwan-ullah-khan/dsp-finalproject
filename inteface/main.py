import streamlit as st
from prediction import prediction_page
from past_predictions import past_predictions_page

API_URL = "..."
CSV_FILE = "user_data.csv"


def main():
    st.set_page_config(page_title="Bank Customer Churn Prediction App", page_icon=":bank:")
    st.write("Welcome to the Bank Customer Churn Prediction App!")

    # sidebar menu for page selection
    page = st.sidebar.selectbox("Select a page", ["Prediction Page", "Past Predictions"])

    # Display the selected page
    if page == "Prediction Page":
        prediction_page(API_URL, CSV_FILE)
    elif page == "Past Predictions":
        past_predictions_page(CSV_FILE)


# Run the main function
if __name__ == "__main__":
    main()
