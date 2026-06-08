import logging
from datetime import datetime

from sqlalchemy import and_, func
from sqlalchemy.orm import Session

from app.models import RawItem

try:
    import ftfy
except Exception:  # pragma: no cover - optional dependency
    ftfy = None

log = logging.getLogger(__name__)


def _fix_encoding(value):
    """Repair recoverable mojibake on stored text so titles/content display cleanly."""
    if not value or ftfy is None:
        return value
    try:
        return ftfy.fix_text(value)
    except Exception:  # pragma: no cover - defensive
        return value


def store_raw_items(db_session: Session, items: list, skip_duplicates: bool = True) -> int:
    """
    Store ingested items to raw_items table with optional deduplication.

    Args:
        db_session: SQLAlchemy session
        items: list of IngestedItem objects
        skip_duplicates: if True, skip items with duplicate URL/content

    Returns:
        Number of items successfully stored
    """
    stored_count = 0

    for item in items:
        try:
            # Check for duplicate by URL if skip_duplicates enabled
            if skip_duplicates and item.url:
                existing = db_session.query(RawItem).filter(
                    and_(
                        RawItem.source_type == item.source_type,
                        RawItem.url == item.url,
                    )
                ).first()

                if existing:
                    log.debug(f"Skipping duplicate URL: {item.url}")
                    continue

            # Extract content_hash from raw_payload for secondary dedup check
            content_hash = None
            if item.raw_payload and "content_hash" in item.raw_payload:
                content_hash = item.raw_payload["content_hash"]

            # Check for duplicate by content hash if available. Use an indexed
            # column query (the content_hash is persisted below) instead of
            # scanning every row in Python — the old approach was O(n^2) per batch
            # and untenable at backfill/multi-company volume.
            if skip_duplicates and content_hash:
                existing = (
                    db_session.query(RawItem.id)
                    .filter(RawItem.content_hash == content_hash)
                    .first()
                )
                if existing:
                    log.debug(f"Skipping duplicate content hash: {content_hash}")
                    continue

            raw_item = RawItem(
                source_type=item.source_type,
                source_name=item.source_name,
                author=item.author,
                title=_fix_encoding(item.title),
                content=_fix_encoding(item.content),
                url=item.url,
                published_at=item.published_at,
                ingested_at=datetime.utcnow(),
                engagement_metrics=item.engagement_metrics,
                company_candidates=item.company_candidates,
                sector_tags=item.sector_tags,
                language=item.language,
                raw_payload=item.raw_payload,
                content_hash=content_hash,
            )
            db_session.add(raw_item)
            stored_count += 1
            log.debug(f"Queued item for storage: {item.title[:60]}...")
        except Exception as exc:
            log.warning(f"Failed to store item {item.title}: {exc}")

    try:
        db_session.commit()
        log.info(f"Successfully stored {stored_count} raw items to database")
    except Exception as exc:
        log.error(f"Commit failed: {exc}")
        db_session.rollback()
        stored_count = 0

    return stored_count


def get_recent_raw_items(
    db_session: Session, company: str = None, limit: int = 20, hours: int = 24
) -> list[RawItem]:
    """Retrieve recent raw items, optionally filtered by company"""
    from sqlalchemy import text

    query = db_session.query(RawItem).filter(
        RawItem.ingested_at >= func.now() - text(f"INTERVAL '{hours} hours'"),
    )

    # Simple string search in company_candidates
    if company:
        query = query.filter(
            RawItem.company_candidates.astext.contains(company.lower())
        )

    return query.order_by(RawItem.ingested_at.desc()).limit(limit).all()
