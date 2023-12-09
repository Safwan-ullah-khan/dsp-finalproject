import great_expectations as gx
import pandas as pd
import os

#FILE = "../../data/customer_churn_records.csv"


def validation_checks(file):
    context = gx.get_context()

    validator = context.sources.pandas_default.read_csv(file)

    validator.expect_column_values_to_be_unique("CustomerId")

    validator.expect_column_values_to_be_in_set(
        "Gender", ["Male", "Female"]
    )
    validator.expect_column_values_to_be_between(
        "Age", min_value=0, max_value=120
    )
    validator.expect_column_values_to_not_be_null(
        column="CustomerId"
    )
    validator.expect_column_values_to_be_between(
        "Age", min_value=0, max_value=120
    )
    validator.expect_column_values_to_be_between(
        "CreditScore", min_value=0
    )
    validator.expect_column_values_to_be_between(
        "Balance", min_value=0
    )
    validator.expect_column_values_to_be_between(
        "NumOfProducts", min_value=1, max_value=5
    )
    validator.expect_column_values_to_be_in_set(
        "CardType", ["SILVER", "GOLD", "PLATINUM", "DIAMOND"]
    )
    validator.expect_column_values_to_be_in_set(
        "SatisfactionScore", [1, 2, 3, 4, 5]
    )
    validator.expect_column_values_to_be_in_set(
        "Exited", [0, 1]
    )
    validator.expect_column_values_to_be_in_set(
        "HasCrCard", [0, 1]
    )
    validator.expect_column_values_to_be_in_set(
        "IsActiveMember", [0, 1]
    )

    validator_result = validator.validate()
    #print(validator_result["success"])

    return validator_result



#validation_checks(FILE)
