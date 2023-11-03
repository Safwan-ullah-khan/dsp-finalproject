from fastapi import FastAPI, Depends
import joblib
import psycopg2
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


@app.post("/predict/")
async def predict(json_data: dict, db: SessionLocal = Depends(get_db)):
    # Get the customer data from the JSON data
    df = pd.DataFrame([json_data])

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

    prediction = model.predict(df)
    result = {"prediction": prediction.tolist()}

    return result


@app.get('/past-predictions/')
def get_predict():
    connection = psycopg2.connect(
        "dbname=mydbs user=postgres password=mynameisraydi112")
    cursor = connection.cursor()
    sql = """SELECT * FROM model_predictions;"""
    cursor.execute(sql)
    predictions = cursor.fetchall()
    connection.commit()
    cursor.close()
    return predictions


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
