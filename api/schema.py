from datetime import date

from pydantic import BaseModel, Field

class CustomerData(BaseModel):
    CreditScore: int

    Gender: str
    Age: int
    Tenure: int
    Balance: float
    NumOfProducts: int
    HasCrCard: int
    IsActiveMember: int
    EstimatedSalary: float

    SatisfactionScore: int
    CardType: str
    PointEarned: int
class DateRange(BaseModel):
    start_date: date
    end_date: date
#class PredictionRequest(BaseModel):
 #   data: list[CustomerData]