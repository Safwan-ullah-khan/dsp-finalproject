from prediction import prediction_page
from past_predictions import past_predictions_page
import streamlit as st

API_URL = "http://127.0.0.1:8050/predict/"
GET_API_URL = "http://127.0.0.1:8050/past-predictions/"


def main():
    st.set_page_config(page_title="Bank Customer Churn Prediction App",
                       page_icon=":bank:")
    st.write("Welcome to the Bank Customer Churn Prediction App!")

    # sidebar menu for page selection
    page = st.sidebar.selectbox("Select a page",
                                ["Prediction Page", "Past Predictions"])

    # Display the selected page
    if page == "Prediction Page":
        prediction_page(API_URL)
    elif page == "Past Predictions":
        past_predictions_page(GET_API_URL)


# Run the main function
if __name__ == "__main__":
    main()
