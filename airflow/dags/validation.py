import shutil

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


def save_files_to_correct_folder(file, folder_b, folder_c):
    validation_result = validation_checks(file)

    if validation_result["success"]:
        shutil.move(file, folder_c)
    else:
        if all(result["success"] is False for result in validation_result["results"]):
            shutil.move(file, folder_b)
        else:
            split_records_problem_files(file, folder_b, folder_c, validation_result)


def split_records_problem_files(file_path, folder_b, folder_c, validation_result):
    df = pd.read_csv(file_path)

    problem_rows = []
    for result in validation_result["results"]:
        if not result["success"]:
            problem_rows.extend(result["result"]["unexpected_index_list"])

    if problem_rows:
        df_problems = df.loc[problem_rows]
        df_no_problems = df.drop(problem_rows)

        problems_file_path = os.path.join(
            folder_b, f"file_with_problems_{os.path.basename(file_path)}"
        )
        df_problems.to_csv(problems_file_path, index=False)

        no_problems_file_path = os.path.join(
            folder_c,
            f"file_without_problems_{os.path.basename(file_path)}",
        )
        df_no_problems.to_csv(no_problems_file_path, index=False)

    else:
        shutil.move(file_path, folder_c)
