from datetime import datetime
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import and_
from db_setup import *
from models import *
from schema import *
import pandas as pd
import joblib


def create_tables():
    Base.metadata.create_all(bind=engine)


def start_application():
    application = FastAPI(title=settings.PROJECT_NAME,
                          version=settings.PROJECT_VERSION)
    create_tables()
    return application


app = start_application()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

model = joblib.load('../notebook/boosting_model.joblib')


@app.post("/predict/")
async def predict(data: CustomerData, db: SessionLocal = Depends(get_db)):
    customer = Customer(**data.dict())

    # Convert Pydantic model to DataFrame
    input_data = pd.DataFrame([data.dict()])
    input_data.drop(
        columns=["PredictionSource"], inplace=True, errors="ignore")

    # Perform prediction
    prediction_result = model.predict(input_data)
    for i in prediction_result.tolist():
        customer.PredictionResult = i

    db.add(customer)
    db.commit()

    return {"prediction": prediction_result.tolist()}


@app.get('/past-predictions/')
def get_predict(dates: dict, db: SessionLocal = Depends(get_db)):

    start_date = dates["start_date"]
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = dates["end_date"]
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    prediction_source = dates["pred_source"]
    print(f"Prediction Source: {prediction_source}")

    if prediction_source == "webpage":
        predictions = db.query(Customer).filter(
            and_(Customer.PredictionDate >= start_date,
                 Customer.PredictionDate < end_date,
                 Customer.PredictionSource == prediction_source)
        ).all()

    if prediction_source == "scheduled":
        predictions = db.query(Customer).filter(
            and_(Customer.PredictionDate >= start_date,
                 Customer.PredictionDate < end_date,
                 Customer.PredictionSource == prediction_source)
        ).all()

    if prediction_source == "all":
        predictions = db.query(Customer).filter(
            and_(Customer.PredictionDate >= start_date,
                 Customer.PredictionDate < end_date)
        ).all()

    return predictions


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8050)
