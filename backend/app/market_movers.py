"""Aggregate notable people ("market movers") across recent articles.

Surfaces the project's notable-person capability dashboard-wide: who is driving
news discussion, how often, and with what average predicted impact. Read-only;
aggregates over a recent published_at window. Reuses the stored
ProcessedItem.notable_people plus the causal impact score (never price data).
"""

from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from app import notable_people as notable_people_mod
from app.analytics import compute_impact_score
from app.models import ProcessedItem, RawItem


def market_movers(
    db_session: Session,
    window_days: int = 7,
    limit: int = 8,
) -> dict[str, object]:
    """Per-person article counts + avg impact over the recent window."""
    window_days = max(1, min(window_days, 90))
    limit = max(1, min(limit, 50))
    since = datetime.utcnow() - timedelta(days=window_days)

    rows = (
        db_session.query(ProcessedItem, RawItem)
        .join(RawItem, ProcessedItem.raw_item_id == RawItem.id)
        .filter(RawItem.published_at.isnot(None))
        .filter(RawItem.published_at >= since)
        .order_by(RawItem.published_at.desc())
        .all()
    )

    agg: dict[str, dict] = {}
    for processed, raw in rows:
        people = processed.notable_people or []
        if not people:
            continue
        impact = compute_impact_score(
            source_type=raw.source_type,
            engagement_metrics=raw.engagement_metrics,
            relevance_score=processed.relevance_score,
            sentiment_score=processed.sentiment_score,
            model_confidence=_confidence(processed.model_confidence),
            published_at=raw.published_at,
            notable_boost=notable_people_mod.notable_boost(people),
        )
        for person in people:
            name = person.get("name")
            if not name:
                continue
            entry = agg.get(name)
            if entry is None:
                entry = {
                    "name": name,
                    "role": person.get("role"),
                    "article_count": 0,
                    "_impact_sum": 0.0,
                    "headline": raw.title,  # rows are desc by published_at → latest first
                    "url": raw.url,
                    "published_at": raw.published_at.isoformat() if raw.published_at else None,
                }
                agg[name] = entry
            entry["article_count"] += 1
            entry["_impact_sum"] += impact

    movers = []
    for entry in agg.values():
        count = entry["article_count"]
        movers.append(
            {
                "name": entry["name"],
                "role": entry["role"],
                "article_count": count,
                "avg_impact": round(entry["_impact_sum"] / count, 4) if count else 0.0,
                "headline": entry["headline"],
                "url": entry["url"],
                "published_at": entry["published_at"],
            }
        )

    most_mentioned = sorted(
        movers, key=lambda m: (m["article_count"], m["avg_impact"]), reverse=True
    )
    highest_impact = sorted(
        movers, key=lambda m: (m["avg_impact"], m["article_count"]), reverse=True
    )

    return {
        "status": "success",
        "window_days": window_days,
        "distinct_people": len(movers),
        "most_mentioned": most_mentioned[:limit],
        "highest_impact": highest_impact[:limit],
    }


def _confidence(model_confidence: object) -> float:
    if isinstance(model_confidence, dict):
        try:
            return float(model_confidence.get("confidence", 0.0) or 0.0)
        except (TypeError, ValueError):
            return 0.0
    return 0.0
