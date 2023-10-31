import io
import numpy as np
from fastapi import FastAPI, HTTPException, UploadFile
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

model = joblib.load('../notebook/boosting_model.joblib')


@app.post("/predict/")
async def predict(file: UploadFile):
    if file.filename.endswith(".csv"):
        contents = await file.read()
        df = pd.read_csv(io.StringIO(contents.decode("utf-8")))

        prediction = model.predict(df)
        result = {"prediction": prediction.tolist()}
        print(result)
        return result



    #except Exception as e:
       # raise HTTPException(status_code=500, detail="Error making prediction")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
#[Creditscore, Geography, Gender, Age, Tenure, Balance, NumofProducts,HasCrCard, IsActiveMember, EstimatedSalary, Complain, Satisfaction_Score,Card_Type , Point_Earned]