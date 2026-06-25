from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    environment: str = "development"
    max_batch_size: int = 10000
    output_dir: str = "output"
    database_url: str = "sqlite:///./pipeline.db"
    pipeline_api_key: str = "change-me"


settings = Settings()
