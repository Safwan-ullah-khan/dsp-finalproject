from pydantic import BaseModel
from typing import Optional


class CustomerData(BaseModel):
    """
    A model representing customer data, used for
    predictions and analytics within the application.

    Attributes:
    CreditScore: The customer's credit score.
    Gender: The customer's gender.
    Age: The customer's age.
    Tenure: The number of years the customer has been with the bank.
    Balance: The customer's account balance.
    NumOfProducts: The number of products the customer has with the bank.
    HasCrCard: Whether the customer has a credit card (1) or not (0).
    IsActiveMember: Whether the customer is an active member (1) or not (0).
    EstimatedSalary: The customer's estimated salary.
    SatisfactionScore: The customer's satisfaction score with the bank.
    CardType: The type of card the customer holds.
    PointEarned: The number of points the customer has earned.
    PredictionSource: The source of the prediction data.
    """
    CreditScore: Optional[int] = None
    Gender: Optional[str] = None
    Age: Optional[int] = None
    Tenure: Optional[int] = None
    Balance: Optional[float] = None
    NumOfProducts: Optional[int] = None
    HasCrCard: Optional[int] = None
    IsActiveMember: Optional[int] = None
    EstimatedSalary: Optional[float] = None
    SatisfactionScore: Optional[int] = None
    CardType: Optional[str] = None
    PointEarned: Optional[int] = None
    PredictionSource: Optional[str] = None
