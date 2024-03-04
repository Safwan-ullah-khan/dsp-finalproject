from email.mime.text import MIMEText
import great_expectations as gx
import smtplib


user_email = "duong.tranhn1102@gmail.com"
recipient_email = "trankhanhduong112@gmail.com"
DB_URL = "postgresql://postgres:khanhduong@host.docker.internal:5432/mydbs"


def send_email(sender, recipient, subject, message):
    # Create the message
    message = MIMEText(message)
    message["Subject"] = subject
    message["From"] = sender
    message["To"] = recipient

    # Establish a connection with the SMTP server
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(user_email, "ulws pdlo avlh oggs")
        server.sendmail(sender, recipient, message.as_string())


def gx_validation(file):
    context = gx.get_context()
    validator = context.sources.pandas_default.read_csv(file)

    validator.expect_column_values_to_be_in_set(
        "Gender", ["Male", "Female"],
        result_format={'result_format': 'SUMMARY'}
    )

    validator.expect_column_values_to_be_of_type(
        "Age", "int64",
        result_format={'result_format': 'SUMMARY'}
    )

    validator.expect_column_values_to_be_between(
        "Age", min_value=0, max_value=120,
        result_format={'result_format': 'SUMMARY'}
    )

    validator.expect_column_values_to_be_of_type(
        "Tenure", "int64",
        result_format={'result_format': 'SUMMARY'}
    )

    validator.expect_column_values_to_be_between(
        "Tenure", min_value=0,
        result_format={'result_format': 'SUMMARY'}
    )

    validator.expect_column_values_to_be_of_type(
        "CreditScore", "int64",
        result_format={'result_format': 'SUMMARY'}
    )

    validator.expect_column_values_to_be_between(
        "CreditScore", min_value=0,
        result_format={'result_format': 'SUMMARY'}
    )

    validator.expect_column_values_to_be_of_type(
        "Balance", "float64",
        result_format={'result_format': 'SUMMARY'}
    )

    validator.expect_column_values_to_be_between(
        "Balance", min_value=0.0,
        result_format={'result_format': 'SUMMARY'}
    )

    validator.expect_column_values_to_be_of_type(
        "NumOfProducts", "int64",
        result_format={'result_format': 'SUMMARY'}
    )

    validator.expect_column_values_to_be_between(
        "NumOfProducts", min_value=1, max_value=5,
        result_format={'result_format': 'SUMMARY'}
    )

    validator.expect_column_values_to_be_of_type(
        "CardType", "object",
        result_format={'result_format': 'SUMMARY'}
    )

    validator.expect_column_values_to_be_in_set(
        "CardType", ["SILVER", "GOLD", "PLATINUM", "DIAMOND"],
        result_format={'result_format': 'SUMMARY'}
    )

    validator.expect_column_values_to_be_of_type(
        "SatisfactionScore", "int64",
        result_format={'result_format': 'SUMMARY'}
    )

    validator.expect_column_values_to_be_in_set(
        "SatisfactionScore", [1, 2, 3, 4, 5],
        result_format={'result_format': 'SUMMARY'}
    )

    validator.expect_column_values_to_be_of_type(
        "HasCrCard", "int64",
        result_format={'result_format': 'SUMMARY'}
    )

    validator.expect_column_values_to_be_in_set(
        "HasCrCard", [0, 1], result_format={'result_format': 'SUMMARY'}
    )

    validator.expect_column_values_to_be_of_type(
        "IsActiveMember", "int64",
        result_format={'result_format': 'SUMMARY'}
    )

    validator.expect_column_values_to_be_in_set(
        "IsActiveMember", [0, 1],
        result_format={'result_format': 'SUMMARY'}
    )

    validator.expect_column_values_to_be_of_type(
        "EstimatedSalary", "float64",
        result_format={'result_format': 'SUMMARY'}
    )

    validator.expect_column_values_to_be_between(
        "EstimatedSalary", min_value=0.0,
        result_format={'result_format': 'SUMMARY'}
    )

    validator.expect_column_values_to_be_of_type(
        "PointEarned", "int64",
        result_format={'result_format': 'SUMMARY'}
    )

    validator.expect_column_values_to_be_between(
        "PointEarned", min_value=0,
        result_format={'result_format': 'SUMMARY'}
    )

    validator_result = validator.validate()
    return {"file": file, "validator_result": validator_result}
