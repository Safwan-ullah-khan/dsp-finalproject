from sqlalchemy import Column, DateTime, Integer, String, Float
from sqlalchemy.sql import func
from typing import Any
from sqlalchemy.orm import as_declarative

@as_declarative()
class Base:
    id: Any
    __name__: str


class Customer(Base):
    __tablename__ = 'customer_data'

    CustomerId = Column(Integer, primary_key=True)
    CreditScore = Column(Integer)
    Gender = Column(String)
    Age = Column(Integer)
    Tenure = Column(Integer)
    Balance = Column(Float)
    NumOfProducts = Column(Integer)
    HasCrCard = Column(Integer)
    IsActiveMember = Column(Integer)
    EstimatedSalary = Column(Float)
    SatisfactionScore = Column(Integer)
    CardType = Column(String)
    PointEarned = Column(Integer)
    PredictionResult = Column(Integer)
    PredictionDate = Column(DateTime, server_default=func.now())
    PredictionSource = Column(String)
'''class ModelPrediction(Base):
    __tablename__ = 'model_predictions'

    PredictionId = Column(Integer, primary_key=True)
    PredictionResult = Column(Integer)
    PredictionDate = Column(DateTime, server_default=func.now())'''





