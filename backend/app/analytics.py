from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta
from math import sqrt

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models import ProcessedItem, RawItem, SectorCorrelation, TimeSeriesSentiment


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

    return 0.3 if st == "rss" else 0.35


def normalize_entity_relevance(relevance_score: float) -> float:
    return _clamp_0_1(relevance_score)


def impact_component_breakdown(
    source_type: str,
    engagement_metrics: dict | None,
    relevance_score: float,
) -> dict[str, float]:
    reliability = source_reliability_score(source_type)
    engagement = normalize_engagement(source_type, engagement_metrics)
    relevance = normalize_entity_relevance(relevance_score)

    reliability_contrib = 0.45 * reliability
    engagement_contrib = 0.30 * engagement
    relevance_contrib = 0.25 * relevance

    return {
        "reliability": round(reliability, 6),
        "engagement": round(engagement, 6),
        "relevance": round(relevance, 6),
        "reliability_contribution": round(reliability_contrib, 6),
        "engagement_contribution": round(engagement_contrib, 6),
        "relevance_contribution": round(relevance_contrib, 6),
        "impact_score": round(
            _clamp_0_1(reliability_contrib + engagement_contrib + relevance_contrib),
            6,
        ),
    }


def compute_impact_score(source_type: str, engagement_metrics: dict | None, relevance_score: float) -> float:
    breakdown = impact_component_breakdown(source_type, engagement_metrics, relevance_score)
    return float(breakdown["impact_score"])


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


def _safe_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def _confidence_score(item_count: int, volatility: float) -> float:
    volume_component = min(max(item_count, 0) / 8.0, 1.0)
    volatility_component = 1.0 - _clamp_0_1(volatility / 1.2)
    confidence = (0.65 * volume_component) + (0.35 * volatility_component)
    return round(_clamp_0_1(confidence), 6)


def _fetch_processed_with_raw(
    db_session: Session,
    company: str,
    since: datetime,
) -> list[tuple[ProcessedItem, RawItem]]:
    normalized_company = company.strip().lower()
    return (
        db_session.query(ProcessedItem, RawItem)
        .join(RawItem, ProcessedItem.raw_item_id == RawItem.id)
        .filter(func.lower(ProcessedItem.company) == normalized_company)
        .filter(ProcessedItem.processed_at >= since)
        .order_by(ProcessedItem.processed_at.asc())
        .all()
    )


def _resolve_company_name(db_session: Session, company: str) -> str:
    normalized_company = company.strip().lower()
    row = (
        db_session.query(ProcessedItem.company)
        .filter(func.lower(ProcessedItem.company) == normalized_company)
        .order_by(ProcessedItem.processed_at.desc())
        .first()
    )
    if row and row[0]:
        return str(row[0])
    return company.strip()


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

    resolved_company = _resolve_company_name(db_session, company)
    rows = _fetch_processed_with_raw(db_session, company=resolved_company, since=since)
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

    db_session.query(TimeSeriesSentiment).filter(
        func.lower(TimeSeriesSentiment.company) == resolved_company.lower(),
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
        confidence = _confidence_score(len(entries), volatility)
        end = _bucket_end(start, bucket)

        agg_row = TimeSeriesSentiment(
            company=resolved_company,
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
                "confidence_score": confidence,
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

    resolved_company = _resolve_company_name(db_session, company)
    rows = _fetch_processed_with_raw(db_session, company=resolved_company, since=since)
    ranked: list[dict] = []
    for processed, raw in rows:
        breakdown = impact_component_breakdown(
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
                "impact_score": float(breakdown["impact_score"]),
                "impact_factors": breakdown,
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
    resolved_company = _resolve_company_name(db_session, company)

    if recompute:
        return aggregate_timeline(
            db_session=db_session,
            company=resolved_company,
            bucket=bucket,
            window_days=window_days,
        )

    rows = (
        db_session.query(TimeSeriesSentiment)
        .filter(func.lower(TimeSeriesSentiment.company) == resolved_company.lower())
        .filter(TimeSeriesSentiment.bucket_size == bucket)
        .filter(TimeSeriesSentiment.bucket_start >= _bucket_start(since, bucket))
        .order_by(TimeSeriesSentiment.bucket_start.asc())
        .all()
    )

    if not rows:
        return aggregate_timeline(
            db_session=db_session,
            company=resolved_company,
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
            "confidence_score": _confidence_score(int(row.item_count), float(row.volatility_proxy)),
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
    resolved_company = _resolve_company_name(db_session, company)
    timeline = get_timeline(
        db_session=db_session,
        company=resolved_company,
        bucket=bucket,
        window_days=window_days,
        recompute=recompute_timeline,
    )
    items = get_ranked_items(
        db_session=db_session,
        company=resolved_company,
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
        "company": resolved_company,
        "bucket": bucket,
        "window_days": window_days,
        "timeline_points": len(timeline),
        "items_returned": len(items),
        "average_weighted_sentiment": round(avg_sentiment, 6),
        "timeline": timeline,
        "items": items,
    }


def _pearson(xs: list[float], ys: list[float]) -> float:
    if len(xs) != len(ys) or len(xs) < 2:
        return 0.0

    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    den_x = sqrt(sum((x - mean_x) ** 2 for x in xs))
    den_y = sqrt(sum((y - mean_y) ** 2 for y in ys))
    if den_x == 0.0 or den_y == 0.0:
        return 0.0

    return round(max(-1.0, min(1.0, num / (den_x * den_y))), 6)


def _rank(values: list[float]) -> list[float]:
    indexed = list(enumerate(values))
    indexed.sort(key=lambda pair: pair[1])

    ranks = [0.0] * len(values)
    i = 0
    while i < len(indexed):
        j = i
        while j + 1 < len(indexed) and indexed[j + 1][1] == indexed[i][1]:
            j += 1

        avg_rank = (i + j + 2) / 2.0
        for k in range(i, j + 1):
            ranks[indexed[k][0]] = avg_rank
        i = j + 1

    return ranks


def _spearman(xs: list[float], ys: list[float]) -> float:
    if len(xs) != len(ys) or len(xs) < 2:
        return 0.0
    return _pearson(_rank(xs), _rank(ys))


def _correlation(xs: list[float], ys: list[float], method: str) -> float:
    if method == "spearman":
        return _spearman(xs, ys)
    return _pearson(xs, ys)


def _best_lag(
    x: list[float],
    y: list[float],
    method: str,
    max_lag: int,
) -> tuple[int, float]:
    best_lag = 0
    best_corr = _correlation(x, y, method)

    for lag in range(-max_lag, max_lag + 1):
        if lag == 0:
            continue

        if lag > 0:
            x_slice = x[:-lag]
            y_slice = y[lag:]
        else:
            lag_abs = abs(lag)
            x_slice = x[lag_abs:]
            y_slice = y[:-lag_abs]

        if len(x_slice) < 2 or len(y_slice) < 2:
            continue

        corr = _correlation(x_slice, y_slice, method)
        if abs(corr) > abs(best_corr):
            best_corr = corr
            best_lag = lag

    return best_lag, round(best_corr, 6)


def get_sector_insights(
    db_session: Session,
    bucket: str = "day",
    window_days: int = 14,
    method: str = "pearson",
    max_lag: int = 3,
    recompute: bool = False,
) -> dict[str, object]:
    bucket = bucket.lower().strip()
    method = method.lower().strip()

    if bucket not in {"hour", "day"}:
        raise ValueError("bucket must be one of: hour, day")
    if method not in {"pearson", "spearman"}:
        raise ValueError("method must be one of: pearson, spearman")

    window_days = max(1, min(window_days, 90))
    max_lag = max(1, min(max_lag, 14))

    if not recompute:
        existing = (
            db_session.query(SectorCorrelation)
            .filter(SectorCorrelation.bucket_size == bucket)
            .filter(SectorCorrelation.window_days == window_days)
            .filter(SectorCorrelation.method == method)
            .order_by(SectorCorrelation.generated_at.desc())
            .first()
        )

        if existing:
            return {
                "status": "success",
                "bucket": bucket,
                "window_days": window_days,
                "method": method,
                "max_lag": existing.max_lag,
                "sector_count": len(existing.sector_series or []),
                "sector_series": existing.sector_series or [],
                "correlation_matrix": existing.correlation_matrix or [],
                "lead_lag": existing.lead_lag_table or [],
                "generated_at": existing.generated_at.isoformat(),
            }

    since = datetime.utcnow() - timedelta(days=window_days)
    rows = (
        db_session.query(ProcessedItem, RawItem)
        .join(RawItem, ProcessedItem.raw_item_id == RawItem.id)
        .filter(ProcessedItem.processed_at >= since)
        .order_by(ProcessedItem.processed_at.asc())
        .all()
    )

    sector_buckets: dict[str, dict[datetime, list[dict[str, float]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    sector_company_counts: dict[str, set[str]] = defaultdict(set)

    for processed, raw in rows:
        sectors = _safe_list(raw.sector_tags)
        if not sectors:
            sectors = ["Uncategorized"]

        point_bucket = _bucket_start(processed.processed_at, bucket)
        impact = compute_impact_score(
            source_type=raw.source_type,
            engagement_metrics=raw.engagement_metrics,
            relevance_score=processed.relevance_score,
        )
        sentiment = float(processed.sentiment_score)

        for sector in sectors:
            sector_buckets[sector][point_bucket].append(
                {
                    "sentiment": sentiment,
                    "impact": impact,
                }
            )
            sector_company_counts[sector].add(processed.company)

    if not sector_buckets:
        return {
            "status": "success",
            "bucket": bucket,
            "window_days": window_days,
            "method": method,
            "max_lag": max_lag,
            "sector_count": 0,
            "sector_series": [],
            "correlation_matrix": [],
            "lead_lag": [],
            "generated_at": datetime.utcnow().isoformat(),
        }

    all_bucket_points = sorted(
        {point for per_sector in sector_buckets.values() for point in per_sector.keys()}
    )
    sector_series: list[dict[str, object]] = []
    sector_vectors: dict[str, list[float]] = {}

    for sector in sorted(sector_buckets.keys()):
        points = []
        vector: list[float] = []
        total_items = 0

        for bucket_point in all_bucket_points:
            entries = sector_buckets[sector].get(bucket_point, [])
            if entries:
                total_items += len(entries)
                impact_sum = sum(e["impact"] for e in entries)
                if impact_sum > 0:
                    weighted_sentiment = sum(e["sentiment"] * e["impact"] for e in entries) / impact_sum
                else:
                    weighted_sentiment = sum(e["sentiment"] for e in entries) / len(entries)
            else:
                weighted_sentiment = 0.0

            rounded = round(float(weighted_sentiment), 6)
            vector.append(rounded)
            points.append(
                {
                    "bucket_start": bucket_point.isoformat(),
                    "weighted_sentiment": rounded,
                    "item_count": len(entries),
                }
            )

        sector_vectors[sector] = vector
        avg_sentiment = sum(vector) / len(vector) if vector else 0.0
        sector_series.append(
            {
                "sector": sector,
                "company_count": len(sector_company_counts[sector]),
                "item_count": total_items,
                "avg_sentiment": round(avg_sentiment, 6),
                "points": points,
            }
        )

    sector_names = [row["sector"] for row in sector_series]
    correlation_matrix: list[dict[str, object]] = []
    lead_lag: list[dict[str, object]] = []

    for sector_a in sector_names:
        values = []
        for sector_b in sector_names:
            values.append(_correlation(sector_vectors[sector_a], sector_vectors[sector_b], method))
        correlation_matrix.append({"sector": sector_a, "values": values})

    for index, sector_a in enumerate(sector_names):
        for sector_b in sector_names[index + 1 :]:
            lag, corr = _best_lag(
                sector_vectors[sector_a],
                sector_vectors[sector_b],
                method,
                max_lag,
            )

            if lag > 0:
                leader = sector_a
                follower = sector_b
            elif lag < 0:
                leader = sector_b
                follower = sector_a
            else:
                leader = "none"
                follower = "none"

            lead_lag.append(
                {
                    "sector_a": sector_a,
                    "sector_b": sector_b,
                    "best_lag": lag,
                    "correlation": corr,
                    "leader": leader,
                    "follower": follower,
                }
            )

    lead_lag.sort(key=lambda row: abs(float(row["correlation"])), reverse=True)

    snapshot = SectorCorrelation(
        bucket_size=bucket,
        window_days=window_days,
        method=method,
        max_lag=max_lag,
        sector_series=sector_series,
        correlation_matrix=correlation_matrix,
        lead_lag_table=lead_lag,
    )
    db_session.add(snapshot)
    db_session.commit()

    return {
        "status": "success",
        "bucket": bucket,
        "window_days": window_days,
        "method": method,
        "max_lag": max_lag,
        "sector_count": len(sector_series),
        "sector_series": sector_series,
        "correlation_matrix": correlation_matrix,
        "lead_lag": lead_lag,
        "generated_at": snapshot.generated_at.isoformat(),
    }