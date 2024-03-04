from dotenv import load_dotenv
from pathlib import Path
import os


env_path = Path('.') / 'myenv.env'
load_dotenv(dotenv_path=env_path)


class Settings:
    """
    Configuration settings for the application, including project details
    and database connection parameters, sourced from environment variables.

    Attributes:
    PROJECT_NAME (str): Name of the project.
    PROJECT_VERSION (str): Current version of the project.
    POSTGRES_USER (str): Username for the PostgreSQL database.
    POSTGRES_PASSWORD (str): Password for the PostgreSQL database.
    POSTGRES_SERVER (str): Hostname of the PostgreSQL server.
    POSTGRES_PORT (str): Port number for the PostgreSQL server.
    POSTGRES_DB (str): Name of the PostgreSQL database to use.
    DATABASE_URL (str): Full database connection URL.
    """

    PROJECT_NAME: str = "Job Board"
    PROJECT_VERSION: str = "1.0.0"

    POSTGRES_USER: str = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_SERVER: str = os.getenv("POSTGRES_SERVER", "localhost")
    POSTGRES_PORT: str = os.getenv("POSTGRES_PORT", 5432)
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "tdd")
    DATABASE_URL: str = (f"postgresql://"
                         f"{POSTGRES_USER}:"
                         f"{POSTGRES_PASSWORD}@{POSTGRES_SERVER}:"
                         f"{POSTGRES_PORT}/{POSTGRES_DB}")


settings = Settings()
