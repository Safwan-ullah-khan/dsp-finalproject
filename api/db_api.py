import io
import pandas as pd
import psycopg2
import joblib
from fastapi import FastAPI, UploadFile, HTTPException
from pydantic import BaseModel
from datetime import datetime

app = FastAPI()

# Load your machine learning model
model = joblib.load('../notebook/boosting_model.joblib')

# Database connection parameters
db_params = {
    'dbname': 'dspproject',
    'user': 'postgres',
    'password': 'massi',
    'host': '127.0.0.1',
    'port': '5432'
}

# Define a Pydantic model for the expected data in the uploaded CSV
class CustomerDataModel(BaseModel):
    CustomerId: int
    Surname: str
    CreditScore: int
    Geography: str
    Gender: str
    Age: int
    Tenure: int
    Balance: float
    NumOfProducts: int
    HasCrCard: int
    IsActiveMember: int
    EstimatedSalary: float
    Exited: int
    Complain: int
    SatisfactionScore: int = None  # Marked as optional
    CardType: str = None  # Marked as optional
    PointEarned: int = None  # Marked as optional

def store_customer_data(data):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        # Insert the customer data into the customer_data table
        for item in data:
            cursor.execute(
                "INSERT INTO customer_data (CustomerId, Surname, CreditScore, Geography, Gender, Age, Tenure, Balance, NumOfProducts, HasCrCard, IsActiveMember, EstimatedSalary, Exited, Complain, SatisfactionScore, CardType, PointEarned) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    item.CustomerId, item.Surname, item.CreditScore, item.Geography, item.Gender,
                    item.Age, item.Tenure, item.Balance, item.NumOfProducts, item.HasCrCard, item.IsActiveMember,
                    item.EstimatedSalary, item.Exited, item.Complain, item.SatisfactionScore, item.CardType, item.PointEarned
                )
            )

        # Commit the changes and close the database connection
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error storing customer data in the database: {str(e)}")

def store_predictions(customer_ids, predictions):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        # Insert the prediction results into the model_predictions table
        for customer_id, prediction in zip(customer_ids, predictions):
            cursor.execute(
                "INSERT INTO model_predictions (CustomerId, PredictionResult, PredictionDate) "
                "VALUES (%s, %s, %s)",
                (customer_id, float(prediction), datetime.now())
            )

        # Commit the changes and close the database connection
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error storing prediction in the database: {str(e)}")

@app.post("/upload-customer-data/")
async def upload_customer_data(file: UploadFile):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be in CSV format")

    try:
        # Read and parse the CSV file data into a list of CustomerDataModel objects
        contents = await file.read()
        df = pd.read_csv(io.StringIO(contents.decode("utf-8")))
        customer_data = df.to_dict(orient='records')
        data_to_insert = [CustomerDataModel(**item) for item in customer_data]

        # Store the customer data in the database
        store_customer_data(data_to_insert)

        return {"message": "Customer data uploaded and stored successfully"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error uploading customer data: {str(e)}")

@app.post("/predict/")
async def predict(file: UploadFile):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be in CSV format")

    try:
        # Read and parse the CSV file data
        contents = await file.read()
        df = pd.read_csv(io.StringIO(contents.decode("utf-8")))

        # Extract the 'CustomerId' column to associate predictions
        customer_ids = df['CustomerId'].tolist()

        # Make predictions using the model
        df = df.drop(columns=['CustomerId'])  # Remove 'CustomerId' for prediction
        predictions = model.predict(df)

        # Store the predictions in the database
        store_predictions(customer_ids, predictions)

        return {"message": "Predictions stored successfully"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error making predictions: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
