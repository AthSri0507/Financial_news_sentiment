from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = Field(default="Financial Sentiment API")
    environment: str = Field(default="local")
    api_version: str = Field(default="0.1.0")

    database_url: str | None = Field(default=None)
    ingest_token: str | None = Field(default=None)

    # API Keys for connectors
    newsapi_key: str | None = Field(default=None)
    reddit_client_id: str | None = Field(default=None)
    reddit_client_secret: str | None = Field(default=None)
    x_bearer_token: str | None = Field(default=None)

    # NLP enrichment pipeline settings
    nlp_prefer_finbert: bool = Field(default=False)
    nlp_max_items_per_run: int = Field(default=30)
    nlp_max_text_chars: int = Field(default=6000)
    nlp_min_relevance: float = Field(default=0.25)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
