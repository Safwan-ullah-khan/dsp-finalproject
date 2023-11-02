import json

import streamlit as st
import requests
import pandas as pd
from utils import load_data, get_prediction, display_prediction


# Create the prediction page
def prediction_page(api_url, csv_file):
    st.subheader("Bank Customer Churn Prediction")

    prediction_mode = st.radio("Select Prediction Mode", ["Single Sample Prediction", "Multiple Predictions from CSV"])

    if prediction_mode == "Single Sample Prediction":
        st.write("Please enter the details for a single sample prediction:")

        # Input fields for a single sample prediction
        credit_score = int(st.number_input("Credit Score", help="Enter a number between 0 and 900", step=1))
        gender = st.radio("Gender", ["Male", "Female"], help="Select your gender")
        age = int(st.number_input("Age", help="Enter your age", step=1))
        tenure = int(st.number_input("Tenure", help="Enter the number of years you have been with the bank", step=1))
        balance = float(st.number_input("Balance", help="Enter your account balance"))
        num_of_products = int(st.number_input("Number of Products", help="Enter the number of products you have with "
                                                                         "the bank", step=1))
        has_cr_card = st.checkbox("Has Credit Card", help="Check if you have a credit card with the bank")
        is_active_member = st.checkbox("Is Active Member", help="Check if you are an active member of the bank")
        estimated_salary = float(st.number_input("Estimated Salary", help="Enter your estimated salary"))
        satisfaction_score = int(st.slider("Satisfaction Score", 0, 5, help="Select your satisfaction score with the "
                                                                            "bank", step=1))
        card_type = st.selectbox("Card Type", ["DIAMOND", "GOLD", "SILVER", "PLATINUM"], help="Select your card type")
        point_earned = int(st.number_input("Points Earned", help="Enter the number of points you have earned", step=1))

        # Predict button for single sample prediction
        if st.button("Predict Single Sample"):
            # Create a DataFrame with user input data for a single sample
            user_data = pd.DataFrame({
                "Credit Score": [credit_score],
                "Gender": [gender],
                "Age": [age],
                "Tenure": [tenure],
                "Balance": [balance],
                "Number of Products": [num_of_products],
                "Has Credit Card": [has_cr_card],
                "Is Active Member": [is_active_member],
                "Estimated Salary": [estimated_salary],
                "Satisfaction Score": [satisfaction_score],
                "Card Type": [card_type],
                "Points Earned": [point_earned]
            })
            user_data.to_csv("user_data.csv", index=False)

            # Append user data to the CSV file
            df = load_data(csv_file)

            df_json = df.to_json(orient="records")

            # Make predictions using the API URL for a single sample
            sample_data = {
                "CreditScore": credit_score,
                "Gender": gender,
                "Age": age,
                "Tenure": tenure,
                "Balance": balance,
                "NumOfProducts": num_of_products,
                "HasCrCard": has_cr_card,
                "IsActiveMember": is_active_member,
                "EstimatedSalary": estimated_salary,
                "SatisfactionScore": satisfaction_score,
                "CardType": card_type,
                "PointsEarned": point_earned
            }
            prediction_result = get_prediction(api_url, df_json)

            # Display the prediction result for the single sample
            display_prediction(user_data, [prediction_result])

    elif prediction_mode == "Multiple Predictions from CSV":
        st.write("Please upload a CSV file with multiple samples for predictions.")

        # Upload a CSV file for making multiple predictions
        uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])

        if uploaded_file is not None:
            # Make predictions using the uploaded CSV file
            uploaded_data = pd.read_csv(uploaded_file)

            # Make predictions for each row in the uploaded CSV
            predictions = []

            for index, row in uploaded_data.iterrows():
                prediction_data = {
                    "CreditScore": row["Credit Score"],
                    "Gender": row["Gender"],
                    "Age": row["Age"],
                    "Tenure": row["Tenure"],
                    "Balance": row["Balance"],
                    "NumOfProducts": row["Number of Products"],
                    "HasCrCard": row["Has Credit Card"],
                    "IsActiveMember": row["Is Active Member"],
                    "EstimatedSalary": row["Estimated Salary"],
                    "SatisfactionScore": row["Satisfaction Score"],
                    "CardType": row["Card Type"],
                    "PointsEarned": row["Points Earned"]
                }
                prediction_result = get_prediction(api_url, prediction_data)
                predictions.append(prediction_result)

            # Display the predictions for the uploaded CSV file
            display_prediction(uploaded_data, predictions)