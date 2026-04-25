from pathlib import Path

from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict


load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent

SHA_PAYMENTS_DATA_PATH = (
    PROJECT_ROOT
    / "datasets"
    / "sha-portal-data"
    / "sha_payments_combined.xlsx"
)

EXPORT_DIR = PROJECT_ROOT / "exports"
EXPORT_DIR.mkdir(exist_ok=True)

class Settings(BaseSettings):

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    facilities_database_url: str


settings = Settings()
