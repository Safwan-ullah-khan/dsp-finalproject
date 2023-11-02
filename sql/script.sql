-- Active: 1698770219335@@127.0.0.1@5432@dspproject

-- Create table for the customer data 
CREATE TABLE customer_data (
    CustomerId integer PRIMARY KEY,
    Surname text,
    CreditScore integer,
    Geography text,
    Gender text,
    Age integer,
    Tenure integer,
    Balance numeric,
    NumOfProducts integer,
    HasCrCard integer,
    IsActiveMember integer,
    EstimatedSalary numeric,
    Exited integer,
    Complain integer,
    SatisfactionScore integer,
    CardType text,
    PointEarned integer
);

-- Create table for the model predection result 
CREATE TABLE model_predictions (
    PredictionId serial PRIMARY KEY,
    CustomerId integer REFERENCES customer_data(CustomerId),
    PredictionResult numeric,
    PredictionDate timestamp
);


-- Some queries test 
SELECT * FROM customer_data;
