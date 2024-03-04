from datetime import datetime
from typing import List
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import and_
from db_setup import *
from models import Base, Customer
from schema import CustomerData
from enum import Enum
import pandas as pd
import joblib
import uvicorn


class PredictionSource(Enum):
    webpage = "webpage"
    scheduled = "scheduled"
    all = "all"

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
async def predict(data: List[CustomerData],
                  db: SessionLocal = Depends(get_db)):
    """ Perform prediction and store the results in the database
    
    Arguments:
    data: List of CustomerData Pydantic models.
    db: Database session.
    """
    # Convert the list of Pydantic models to a list of dictionaries
    data_dicts = [item.dict() for item in data]

    # Create a DataFrame from the list of dictionaries
    input_data = pd.DataFrame(data_dicts)
    input_data.drop(
        columns=["PredictionSource"], inplace=True, errors="ignore")

    # Perform prediction
    prediction_results = model.predict(input_data)

    results = []
    for idx, prediction in enumerate(prediction_results):
        customer_data = data_dicts[idx]
        customer_data.update({"PredictionResult": int(prediction)})
        # Create and add CustomerModel instance to database
        customer = Customer(**customer_data)
        db.add(customer)
        results.append(customer_data)

    db.commit()
    return {"prediction": results}


@app.get('/past-predictions/')
def get_predict(dates: dict, db: SessionLocal = Depends(get_db)):
    """ Get past predictions from the database
    
    Arguments:
    dates: Dictionary containing the start and end dates for the query.
    db: Database session.
    """
    start_date = datetime.strptime(dates["start_date"], "%Y-%m-%d").date()
    end_date = datetime.strptime(dates["end_date"], "%Y-%m-%d").date()
    prediction_source = PredictionSource(dates["pred_source"])

    filters = [Customer.PredictionDate >= start_date, Customer.PredictionDate < end_date]
    if prediction_source != PredictionSource.all:
        filters.append(Customer.PredictionSource == prediction_source.value)

    predictions = db.query(Customer).filter(and_(*filters)).all()

    return predictions


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8050, reload=True)
