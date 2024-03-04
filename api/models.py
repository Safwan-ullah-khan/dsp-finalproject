from sqlalchemy import Column, DateTime, Integer, String, Float
from sqlalchemy.sql import func
from typing import Any
from sqlalchemy.orm import as_declarative


@as_declarative()
class Base:
    """
    Base class for model entities, providing common attributes.

    Attributes:
    id: Generic identifier for model instances.
    __name__: A placeholder for the model's name.
    """
    id: Any
    __name__: str


class Customer(Base):
    """
    Represents customer data in the database.

    Attributes:
    CustomerId: Primary key, unique identifier for the customer.
    CreditScore: Customer's credit score.
    Gender: Customer's gender.
    Age: Customer's age.
    Tenure: Number of years the customer has been with the bank.
    Balance: Customer's account balance.
    NumOfProducts: Number of bank products used by the customer.
    HasCrCard: Indicates if the customer has a credit card (1) or not (0).
    IsActiveMember: Customer is an active member (1) or not (0).
    EstimatedSalary: Customer's estimated salary.
    SatisfactionScore: Customer's satisfaction score with the bank.
    CardType: Type of card held by the customer.
    PointEarned: Points earned by the customer.
    PredictionResult: Result of the churn prediction.
    PredictionDate: Date when the prediction was made.
    PredictionSource: Source of the prediction data.
    """
    __tablename__ = 'customer_data'

    CustomerId = Column(Integer, autoincrement=True, primary_key=True)
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


class ProblemStats(Base):
    """
    Stores statistics on data quality issues encountered during processing.

    Attributes:
    id: Primary key, auto-incremented.
    file_name: Name of the file where the problem was encountered.
    column: Name of the column with the issue.
    expectation_type: Type of expectation that was not met.
    unexpected_values: Values that caused the failure of expectation.
    error_time: Timestamp when the issue was logged.
    """
    __tablename__ = 'data_problem_stats'

    id = Column(Integer, primary_key=True, autoincrement=True)
    file_name = Column(String)
    column = Column(String)
    expectation_type = Column(String)
    unexpected_values = Column(String)
    error_time = Column(DateTime, server_default=func.now())
