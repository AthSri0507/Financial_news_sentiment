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
    marketaux_api_key: str | None = Field(default=None)
    reddit_client_id: str | None = Field(default=None)
    reddit_client_secret: str | None = Field(default=None)
    huggingface_api_key: str | None = Field(default=None)

    # Sector resolution (cross-sector analytics)
    sector_api_provider: str = Field(default="yfinance")
    sector_api_key: str | None = Field(default=None)

    # Ingestion volume
    ingest_per_source_limit: int = Field(default=25)

    # NLP enrichment pipeline settings
    nlp_prefer_finbert: bool = Field(default=False)
    nlp_finbert_min_confidence: float = Field(default=0.62)
    nlp_max_items_per_run: int = Field(default=100)
    nlp_max_text_chars: int = Field(default=6000)
    nlp_min_relevance: float = Field(default=0.25)

    # Abstractive summarization (HF) — Feature 1
    nlp_prefer_hf_summary: bool = Field(default=False)
    nlp_summary_model_id: str = Field(default="sshleifer/distilbart-cnn-12-6")
    nlp_summary_min_chars: int = Field(default=200)
    nlp_summary_max_input_chars: int = Field(default=1500)

    # Notable-person NER (HF) — Feature 2
    nlp_prefer_hf_ner: bool = Field(default=False)
    nlp_ner_model_id: str = Field(default="dslim/bert-base-NER")

    # Impact scoring (recency) — Feature 3
    impact_recency_halflife_hours: float = Field(default=48.0)

    # Price validation — Feature 4
    price_validation_enabled: bool = Field(default=True)
    price_move_threshold_pct: float = Field(default=2.0)
    price_validation_windows: str = Field(default="1h,4h,1d")
    price_validation_primary_window: str = Field(default="1d")
    price_validation_no_data_grace_days: int = Field(default=5)

    # News freshness (on-view refresh)
    freshness_on_view_enabled: bool = Field(default=True)
    freshness_cooldown_minutes: int = Field(default=15)
    freshness_on_view_limit: int = Field(default=12)

    # Daily paid-API budgets (shared by on-view refresh + scheduled jobs)
    newsapi_daily_quota: int = Field(default=90)
    marketaux_daily_quota: int = Field(default=90)

    # Model energy/carbon estimation (per-inference Wh + grid intensity)
    energy_wh_per_finbert: float = Field(default=0.30)
    energy_wh_per_hf_summary: float = Field(default=0.60)
    energy_wh_per_hf_ner: float = Field(default=0.30)
    energy_wh_per_lexicon_item: float = Field(default=0.0005)
    energy_wh_base_per_item: float = Field(default=0.01)
    energy_grid_gco2_per_kwh: float = Field(default=475.0)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
