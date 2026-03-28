from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta
from math import sqrt

from sqlalchemy.orm import Session

from app.models import ProcessedItem, RawItem, TimeSeriesSentiment


SOURCE_RELIABILITY: dict[str, float] = {
    "marketaux": 0.9,
    "newsapi": 0.82,
    "rss": 0.72,
    "reddit": 0.5,
    "x": 0.5,
}


def _clamp_0_1(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def source_reliability_score(source_type: str) -> float:
    return _clamp_0_1(SOURCE_RELIABILITY.get((source_type or "").lower(), 0.65))


def normalize_engagement(source_type: str, engagement_metrics: dict | None) -> float:
    metrics = engagement_metrics or {}
    st = (source_type or "").lower()

    if st == "reddit":
        upvotes = float(metrics.get("upvotes", 0.0))
        comments = float(metrics.get("comments", 0.0))
        score = (0.7 * min(upvotes / 4000.0, 1.0)) + (0.3 * min(comments / 600.0, 1.0))
        return _clamp_0_1(score)

    if st == "x":
        likes = float(metrics.get("likes", 0.0))
        retweets = float(metrics.get("retweets", 0.0))
        replies = float(metrics.get("replies", 0.0))
        score = (
            (0.45 * min(likes / 3000.0, 1.0))
            + (0.35 * min(retweets / 900.0, 1.0))
            + (0.20 * min(replies / 400.0, 1.0))
        )
        return _clamp_0_1(score)

    if st == "newsapi":
        has_image = 1.0 if metrics.get("has_image") else 0.0
        return _clamp_0_1(0.35 + 0.25 * has_image)

    if st == "marketaux":
        return 0.5

    # RSS and fallback sources generally carry little explicit engagement in payloads.
    return 0.3 if st == "rss" else 0.35


def normalize_entity_relevance(relevance_score: float) -> float:
    return _clamp_0_1(relevance_score)


def compute_impact_score(source_type: str, engagement_metrics: dict | None, relevance_score: float) -> float:
    reliability = source_reliability_score(source_type)
    engagement = normalize_engagement(source_type, engagement_metrics)
    relevance = normalize_entity_relevance(relevance_score)

    impact = (0.45 * reliability) + (0.30 * engagement) + (0.25 * relevance)
    return round(_clamp_0_1(impact), 6)


def _bucket_start(dt: datetime, bucket: str) -> datetime:
    if bucket == "hour":
        return dt.replace(minute=0, second=0, microsecond=0)
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def _bucket_end(start: datetime, bucket: str) -> datetime:
    if bucket == "hour":
        return start + timedelta(hours=1)
    return start + timedelta(days=1)


def _std_dev(values: list[float]) -> float:
    if len(values) <= 1:
        return 0.0
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / len(values)
    return sqrt(var)


def _fetch_processed_with_raw(
    db_session: Session,
    company: str,
    since: datetime,
) -> list[tuple[ProcessedItem, RawItem]]:
    return (
        db_session.query(ProcessedItem, RawItem)
        .join(RawItem, ProcessedItem.raw_item_id == RawItem.id)
        .filter(ProcessedItem.company == company)
        .filter(ProcessedItem.processed_at >= since)
        .order_by(ProcessedItem.processed_at.asc())
        .all()
    )


def aggregate_timeline(
    db_session: Session,
    company: str,
    bucket: str = "day",
    window_days: int = 7,
) -> list[dict]:
    bucket = bucket.lower().strip()
    if bucket not in {"hour", "day"}:
        raise ValueError("bucket must be one of: hour, day")

    window_days = max(1, min(window_days, 90))
    since = datetime.utcnow() - timedelta(days=window_days)

    rows = _fetch_processed_with_raw(db_session, company=company, since=since)
    groups: dict[datetime, list[dict]] = defaultdict(list)

    for processed, raw in rows:
        source_type = raw.source_type or "unknown"
        impact = compute_impact_score(
            source_type=source_type,
            engagement_metrics=raw.engagement_metrics,
            relevance_score=processed.relevance_score,
        )

        key = _bucket_start(processed.processed_at, bucket=bucket)
        groups[key].append(
            {
                "sentiment": float(processed.sentiment_score),
                "impact": impact,
                "source_type": source_type,
            }
        )

    # Rebuild aggregate rows for requested window to keep timeline endpoint deterministic.
    db_session.query(TimeSeriesSentiment).filter(
        TimeSeriesSentiment.company == company,
        TimeSeriesSentiment.bucket_size == bucket,
        TimeSeriesSentiment.bucket_start >= _bucket_start(since, bucket),
    ).delete(synchronize_session=False)

    timeline: list[dict] = []
    for start in sorted(groups.keys()):
        entries = groups[start]
        impacts = [e["impact"] for e in entries]
        sentiments = [e["sentiment"] for e in entries]
        source_mix_counter = Counter(e["source_type"] for e in entries)

        impact_sum = sum(impacts)
        if impact_sum > 0:
            weighted = sum(e["sentiment"] * e["impact"] for e in entries) / impact_sum
        else:
            weighted = sum(sentiments) / max(len(sentiments), 1)

        mean_impact = sum(impacts) / max(len(impacts), 1)
        volatility = _std_dev(sentiments)
        end = _bucket_end(start, bucket)

        agg_row = TimeSeriesSentiment(
            company=company,
            bucket_size=bucket,
            bucket_start=start,
            bucket_end=end,
            weighted_sentiment=round(weighted, 6),
            item_count=len(entries),
            source_mix=dict(source_mix_counter),
            volatility_proxy=round(volatility, 6),
            mean_impact=round(mean_impact, 6),
        )
        db_session.add(agg_row)

        timeline.append(
            {
                "bucket_start": start.isoformat(),
                "bucket_end": end.isoformat(),
                "weighted_sentiment": round(weighted, 6),
                "item_count": len(entries),
                "source_mix": dict(source_mix_counter),
                "volatility_proxy": round(volatility, 6),
                "mean_impact": round(mean_impact, 6),
            }
        )

    db_session.commit()
    return timeline


def get_ranked_items(
    db_session: Session,
    company: str,
    window_days: int = 7,
    limit: int = 20,
) -> list[dict]:
    window_days = max(1, min(window_days, 90))
    limit = max(1, min(limit, 100))
    since = datetime.utcnow() - timedelta(days=window_days)

    rows = _fetch_processed_with_raw(db_session, company=company, since=since)
    ranked: list[dict] = []
    for processed, raw in rows:
        impact = compute_impact_score(
            source_type=raw.source_type,
            engagement_metrics=raw.engagement_metrics,
            relevance_score=processed.relevance_score,
        )

        ranked.append(
            {
                "processed_id": str(processed.id),
                "raw_item_id": str(processed.raw_item_id),
                "company": processed.company,
                "title": raw.title,
                "summary": processed.summary,
                "source_type": raw.source_type,
                "source_name": raw.source_name,
                "url": raw.url,
                "published_at": raw.published_at.isoformat() if raw.published_at else None,
                "processed_at": processed.processed_at.isoformat(),
                "sentiment_label": processed.sentiment_label,
                "sentiment_score": float(processed.sentiment_score),
                "relevance_score": float(processed.relevance_score),
                "impact_score": impact,
            }
        )

    ranked.sort(key=lambda item: item["impact_score"], reverse=True)
    return ranked[:limit]


def get_timeline(
    db_session: Session,
    company: str,
    bucket: str = "day",
    window_days: int = 7,
    recompute: bool = False,
) -> list[dict]:
    bucket = bucket.lower().strip()
    window_days = max(1, min(window_days, 90))
    since = datetime.utcnow() - timedelta(days=window_days)

    if recompute:
        return aggregate_timeline(
            db_session=db_session,
            company=company,
            bucket=bucket,
            window_days=window_days,
        )

    rows = (
        db_session.query(TimeSeriesSentiment)
        .filter(TimeSeriesSentiment.company == company)
        .filter(TimeSeriesSentiment.bucket_size == bucket)
        .filter(TimeSeriesSentiment.bucket_start >= _bucket_start(since, bucket))
        .order_by(TimeSeriesSentiment.bucket_start.asc())
        .all()
    )

    if not rows:
        return aggregate_timeline(
            db_session=db_session,
            company=company,
            bucket=bucket,
            window_days=window_days,
        )

    return [
        {
            "bucket_start": row.bucket_start.isoformat(),
            "bucket_end": row.bucket_end.isoformat(),
            "weighted_sentiment": float(row.weighted_sentiment),
            "item_count": int(row.item_count),
            "source_mix": row.source_mix or {},
            "volatility_proxy": float(row.volatility_proxy),
            "mean_impact": float(row.mean_impact),
            "generated_at": row.generated_at.isoformat(),
        }
        for row in rows
    ]


def run_query(
    db_session: Session,
    company: str,
    bucket: str = "day",
    window_days: int = 7,
    item_limit: int = 20,
    recompute_timeline: bool = False,
) -> dict[str, object]:
    timeline = get_timeline(
        db_session=db_session,
        company=company,
        bucket=bucket,
        window_days=window_days,
        recompute=recompute_timeline,
    )
    items = get_ranked_items(
        db_session=db_session,
        company=company,
        window_days=window_days,
        limit=item_limit,
    )

    avg_sentiment = (
        sum(float(point["weighted_sentiment"]) for point in timeline) / len(timeline)
        if timeline
        else 0.0
    )

    return {
        "status": "success",
        "company": company,
        "bucket": bucket,
        "window_days": window_days,
        "timeline_points": len(timeline),
        "items_returned": len(items),
        "average_weighted_sentiment": round(avg_sentiment, 6),
        "timeline": timeline,
        "items": items,
    }