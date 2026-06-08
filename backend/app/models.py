import uuid
from datetime import datetime

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class RawItem(Base):
    """Raw ingested item from any source"""

    __tablename__ = "raw_items"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_type = Column(String(50), nullable=False, index=True)
    source_name = Column(String(255), nullable=False)
    author = Column(String(255), nullable=True)
    title = Column(String(500), nullable=False)
    content = Column(Text, nullable=True)
    url = Column(String(2000), nullable=False)
    published_at = Column(DateTime, nullable=True, index=True)
    ingested_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    engagement_metrics = Column(JSON, nullable=True)
    company_candidates = Column(JSON, nullable=True, index=True)
    sector_tags = Column(JSON, nullable=True)
    language = Column(String(10), default="en", nullable=False)
    raw_payload = Column(JSON, nullable=True)
    content_hash = Column(String(64), nullable=True, index=True)

    __table_args__ = (
        # Unique constraint on (source_type, source_name, url) to prevent duplicates
        Index(
            "ix_raw_items_source_url",
            "source_type",
            "source_name",
            "url",
            unique=True,
        ),
    )


class ProcessedItem(Base):
    """NLP-enriched view of a raw ingested item"""

    __tablename__ = "processed_items"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    raw_item_id = Column(UUID(as_uuid=True), ForeignKey("raw_items.id"), nullable=False, unique=True, index=True)
    company = Column(String(255), nullable=False, index=True)
    cleaned_text = Column(Text, nullable=False)
    language = Column(String(10), nullable=False, default="en", index=True)
    is_noise = Column(String(10), nullable=False, default="false")

    summary = Column(Text, nullable=False)
    sentiment_label = Column(String(20), nullable=False, index=True)
    sentiment_score = Column(Float, nullable=False)
    relevance_score = Column(Float, nullable=False, index=True)

    entities = Column(JSON, nullable=True)
    notable_people = Column(JSON, nullable=True)
    model_confidence = Column(JSON, nullable=True)
    pipeline_flags = Column(JSON, nullable=True)

    processed_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    __table_args__ = (
        Index("ix_processed_items_company_processed_at", "company", "processed_at"),
    )


class TimeSeriesSentiment(Base):
    """Aggregated impact-weighted sentiment by time bucket."""

    __tablename__ = "time_series_sentiment"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company = Column(String(255), nullable=False, index=True)
    bucket_size = Column(String(20), nullable=False, index=True)  # hour/day
    bucket_start = Column(DateTime, nullable=False, index=True)
    bucket_end = Column(DateTime, nullable=False)

    weighted_sentiment = Column(Float, nullable=False)
    item_count = Column(Integer, nullable=False, default=0)
    source_mix = Column(JSON, nullable=False)
    volatility_proxy = Column(Float, nullable=False, default=0.0)
    mean_impact = Column(Float, nullable=False, default=0.0)

    generated_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    __table_args__ = (
        Index(
            "ix_time_series_company_bucket_unique",
            "company",
            "bucket_size",
            "bucket_start",
            unique=True,
        ),
        Index("ix_time_series_company_bucket_start", "company", "bucket_start"),
    )


class PriceValidation(Base):
    """Post-publication market-reaction check for a processed article.

    STRICTLY separate from the (causal) impact score — this is observational
    evidence only and is never read by impact/ranking/timeline/correlation logic.
    """

    __tablename__ = "price_validations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    processed_item_id = Column(
        UUID(as_uuid=True), ForeignKey("processed_items.id"), nullable=False, index=True
    )
    company = Column(String(255), nullable=False, index=True)
    ticker = Column(String(50), nullable=True)
    validation_window = Column(String(20), nullable=False)  # 1h/4h/1d

    baseline_price = Column(Float, nullable=True)
    window_price = Column(Float, nullable=True)
    price_change_pct = Column(Float, nullable=True)
    moved_market = Column(Boolean, nullable=True)
    validation_confidence = Column(Float, nullable=True)
    price_validated = Column(Boolean, nullable=False, default=False)
    validation_status = Column(String(30), nullable=False, default="pending")

    baseline_at = Column(DateTime, nullable=True)
    window_target_at = Column(DateTime, nullable=False)
    validated_at = Column(DateTime, nullable=True)

    __table_args__ = (
        Index(
            "ix_price_validation_item_window",
            "processed_item_id",
            "validation_window",
            unique=True,
        ),
    )


class CompanyFetchState(Base):
    """Per-company freshness cooldown / last-refresh bookkeeping."""

    __tablename__ = "company_fetch_state"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company = Column(String(255), nullable=False, unique=True, index=True)
    last_attempted_at = Column(DateTime, nullable=True)
    last_success_at = Column(DateTime, nullable=True)
    last_item_count = Column(Integer, nullable=False, default=0)


class ApiUsage(Base):
    """Shared daily paid-API call budget (on-view refresh + cron both record here)."""

    __tablename__ = "api_usage"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    provider = Column(String(50), nullable=False)
    day = Column(String(10), nullable=False)  # YYYY-MM-DD (UTC)
    count = Column(Integer, nullable=False, default=0)

    __table_args__ = (
        Index("ix_api_usage_provider_day", "provider", "day", unique=True),
    )


class AgreementSnapshot(Base):
    """Daily snapshot of the validation-agreement KPI (model quality over time)."""

    __tablename__ = "agreement_snapshots"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    day = Column(String(10), nullable=False)  # YYYY-MM-DD (UTC)
    window = Column(String(20), nullable=False)
    impact_threshold = Column(Float, nullable=False)
    n_high_impact = Column(Integer, nullable=False, default=0)
    n_moved = Column(Integer, nullable=False, default=0)
    agreement_pct = Column(Float, nullable=True)
    generated_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index(
            "ix_agreement_day_window_threshold",
            "day",
            "window",
            "impact_threshold",
            unique=True,
        ),
    )


class CompanySector(Base):
    """Cached company -> sector resolution (curated seed, external API, or manual)."""

    __tablename__ = "company_sectors"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company = Column(String(255), nullable=False, unique=True, index=True)
    ticker = Column(String(50), nullable=True)
    sector = Column(String(100), nullable=False)
    source = Column(String(20), nullable=False, default="api")  # curated/api/manual
    resolved_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class SectorCorrelation(Base):
    """Cross-sector correlation and lead/lag analytics snapshots."""

    __tablename__ = "sector_correlations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    bucket_size = Column(String(20), nullable=False, index=True)
    window_days = Column(Integer, nullable=False)
    method = Column(String(20), nullable=False, index=True)  # pearson/spearman
    max_lag = Column(Integer, nullable=False, default=3)

    sector_series = Column(JSON, nullable=False)
    correlation_matrix = Column(JSON, nullable=False)
    lead_lag_table = Column(JSON, nullable=False)

    generated_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    __table_args__ = (
        Index("ix_sector_corr_bucket_window_method", "bucket_size", "window_days", "method"),
    )
