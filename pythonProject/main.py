import numpy as np
from fastapi import FastAPI, HTTPException
import joblib
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder, MinMaxScaler
import pickle
import pandas as pd

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

model = joblib.load('C:/Users/marja/OneDrive/Documents/EPITA/1st Semester/DSP Project/dsp-finalproject/notebook/xgboost_model.pkl')


@app.get("/predict/")
async def predict(Creditscore: int, Geography: str, Gender: str, Age: int, Tenure: int, Balance: float, NumofProducts: int,
                  HasCrCard: int, IsActiveMember: int, EstimatedSalary: float, Complain: int, Satisfaction_Score: int, Card_Type: str,
                  Point_Earned: int):
    input_data = {
        "CreditScore": [Creditscore],
        "Geography": [Geography],
        "Gender": [Gender],
        "Age": [Age],
        "Tenure": [Tenure],
        "Balance": [Balance],
        "NumofProducts": [NumofProducts],
        "HasCrCard": [HasCrCard],
        "IsActiveMember": [IsActiveMember],
        "EstimatedSalary": [EstimatedSalary],
        "Complain": [Complain],
        "Satisfaction Score": [Satisfaction_Score],
        "Card Type": [Card_Type],
        "Point Earned": [Point_Earned]
    }
    input_df = pd.DataFrame(input_data)

    #try:
    #input_data = np.array([Creditscore, Geography, Gender, Age, Tenure, Balance, NumofProducts,HasCrCard, IsActiveMember,
          #         EstimatedSalary, Complain, Satisfaction_Score,Card_Type , Point_Earned])
    #model = pickle.load(open('C:/Users/marja/OneDrive/Documents/EPITA/1st Semester/DSP Project/dsp-finalproject/notebook/xgboost_model.pkl', 'rb'))

    prediction = model.predict(input_df)

    return {"prediction": prediction.tolist()}



    #except Exception as e:
       # raise HTTPException(status_code=500, detail="Error making prediction")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
#[Creditscore, Geography, Gender, Age, Tenure, Balance, NumofProducts,HasCrCard, IsActiveMember, EstimatedSalary, Complain, Satisfaction_Score,Card_Type , Point_Earned]