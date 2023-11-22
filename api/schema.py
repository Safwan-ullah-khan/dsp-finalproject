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

#class PredictionRequest(BaseModel):
 #   data: list[CustomerData]