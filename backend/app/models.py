import uuid
from datetime import datetime

from sqlalchemy import Column, String, DateTime, JSON, Text, Index, Float, ForeignKey, Integer
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
    content_hash = Column(String(64), nullable=True)

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
