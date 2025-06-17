import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    # Database settings
    DATABASE_URL = os.getenv("DATABASE_URL")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = int(os.getenv("DB_PORT", 5432))
    DB_NAME = os.getenv("DB_NAME", "analytics_db")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")

    # API settings
    # OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

    # App settings
    # APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
    # APP_PORT = int(os.getenv("APP_PORT", 8000))
    # DEBUG = os.getenv("DEBUG", "True").lower() == "true"

    # Chart storage
    CHART_STORAGE_PATH = "storage/charts/"

    @property
    def database_url(self):
        if self.DATABASE_URL:
            return self.DATABASE_URL
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"


settings = Settings()