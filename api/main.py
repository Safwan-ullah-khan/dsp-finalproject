from fastapi import FastAPI, Depends
import joblib
import psycopg2
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from db_setup import *
from models import *
from schema import *


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


@app.post("/predict/")
async def predict(data: CustomerData, db: SessionLocal = Depends(get_db)):
    prediction = []

    customer = Customer(**data.dict())
    #db.add(customer)
    #db.commit()

    input_data = pd.DataFrame(
        [data.dict()])  # Convert Pydantic model to DataFrame

    # Perform prediction
    prediction_result = model.predict(input_data)
    for i in prediction_result.tolist():
        customer.PredictionResult = i
    db.add(customer)
    db.commit()


    #customer.PredictionResult = prediction_result
    #db.add(customer)
    #db.commit()
    # You can now use or return the prediction result as needed
    return {"prediction": prediction_result.tolist()}

@app.get('/past-predictions/')
def get_predict():
    connection = psycopg2.connect(
        "dbname=mydbs user=postgres password=safwan")
    cursor = connection.cursor()
    sql = """SELECT * FROM customer_data;"""
    cursor.execute(sql)
    predictions = cursor.fetchall()
    connection.commit()
    cursor.close()
    return predictions


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
