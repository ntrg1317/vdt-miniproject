import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    def __init__(self):
        # Database settings
        self.DBS_HOST = os.getenv("POSTGRES_SOURCE_HOST", "localhost")
        self.DBS_PORT = int(os.getenv("POSTGRES_SOURCE_PORT", 5432))
        self.DBS_NAME = os.getenv("POSTGRES_SOURCE_DB", "postgres")
        self.DBS_USER = os.getenv("POSTGRES_SOURCE_USER", "postgres")
        self.DBS_PASSWORD = os.getenv("POSTGRES_SOURCE_PASSWORD", "postgres")

        self.DBT_HOST = os.getenv("POSTGRES_TARGET_HOST", "localhost")
        self.DBT_PORT = int(os.getenv("POSTGRES_TARGET_PORT", 5432))
        self.DBT_NAME = os.getenv("POSTGRES_TARGET_DB", "debezium")
        self.DBT_USER = os.getenv("POSTGRES_TARGET_USER", "debezium")
        self.DBT_PASSWORD = os.getenv("POSTGRES_TARGET_PASSWORD", "debezium")

        #API settings
        self.AI_API_KEY = os.getenv("AI_API_KEY")

        # App settings
        # APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
        # APP_PORT = int(os.getenv("APP_PORT", 8000))
        # DEBUG = os.getenv("DEBUG", "True").lower() == "true"

        # Chart storage
        CHART_STORAGE_PATH = "storage/charts/"

        #Dash ID
        self.CHI_TIEU_THANG = 7753
        self.CHI_TIEU_THANG_PHONG_BAN = 7759
        self.THANG = 4702

    def __getattr__(self, name):
        # Fallback to environment variables if attribute not found
        return os.getenv(name)

    @property
    def database_url(self):
        return f"postgresql://{self.DBS_USER}:{self.DBS_PASSWORD}@{self.DBS_HOST}:{self.DBS_PORT}/{self.DBS_NAME}"

    @property
    def target_database_url(self):
        return f"postgresql://{self.DBT_USER}:{self.DBT_PASSWORD}@{self.DBT_HOST}:{self.DBT_PORT}/{self.DBT_NAME}"


settings = Settings()