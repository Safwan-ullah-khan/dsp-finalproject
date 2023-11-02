import io
from datetime import datetime

import json
from fastapi import FastAPI, HTTPException, UploadFile, Depends
import joblib

from fastapi.middleware.cors import CORSMiddleware

import pandas as pd
from db_setup import *
from models import *


def create_tables():
    Base.metadata.create_all(bind=engine)


def start_application():
    app = FastAPI(title=settings.PROJECT_NAME,
                  version=settings.PROJECT_VERSION)
    create_tables()
    return app


app = start_application()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

model = joblib.load('../notebook/boosting_model.joblib')


@app.post("/predict")
async def predict(json_data, db: SessionLocal = Depends(get_db)):
    # Get the customer data from the JSON data
    df = pd.read_json(json_data)
    print(df)
    print(df.columns)
    """ 
    # Create a new customer object
    for _, row in df.iterrows():
        customer = Customer(
            CreditScore=row['CreditScore'],
            Gender=row['Gender'],
            Age=row['Age'],
            Tenure=row['Tenure'],
            Balance=row['Balance'],
            NumOfProducts=row['NumOfProducts'],
            HasCrCard=row['HasCrCard'],
            IsActiveMember=row['IsActiveMember'],
            EstimatedSalary=row['EstimatedSalary'],
            SatisfactionScore=row['Satisfaction Score'],
            CardType=row['Card Type'],
            PointEarned=row['Point Earned']
        )

        # Add the customer to the database
        db.add(customer)

        # Commit the changes to the database
    db.commit()
    prediction = model.predict(df)
    for i in prediction.tolist():
        model_prediction = ModelPrediction(PredictionResult=i)
        db.add(model_prediction)
    db.commit()
    """
    prediction = model.predict(df)
    result = {"prediction": prediction.tolist()}
    print(result)
    return result


@app.post("/past_predict/")
async def get_past_predictions(start_date: str, end_date: str,db: SessionLocal = Depends(get_db)):

    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    predictions = db.query(ModelPrediction).filter(
        ModelPrediction.PredictionDate >= start_date,
        ModelPrediction.PredictionDate <= end_date).all()
    results = []

    for prediction in predictions:
        # Retrieve the corresponding customer data
        customer = db.query(Customer).filter_by(
            CustomerId=prediction.PredictionId).first()


        prediction_data = {
            "CustomerData": {
                "CustomerId": customer.CustomerId,
                "CreditScore": customer.CreditScore,
                "Gender": customer.Gender,
                "Age": customer.Age,
                "Tenure": customer.Tenure,
                "Balance": customer.Balance,
                "NumOfProducts": customer.NumOfProducts,
                "HasCrCard": customer.HasCrCard,
                "IsActiveMember": customer.IsActiveMember,
                "EstimatedSalary": customer.EstimatedSalary,
                "SatisfactionScore": customer.SatisfactionScore,
                "CardType": customer.CardType,
                "PointEarned": customer.PointEarned,
            },
            "PredictionResult": prediction.PredictionResult,
            "PredictionDate": prediction.PredictionDate,
        }
        results.append(prediction_data)

    return results



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
