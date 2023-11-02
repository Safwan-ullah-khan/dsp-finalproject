from pydantic import BaseModel

class CustomerFeatures(BaseModel):
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

