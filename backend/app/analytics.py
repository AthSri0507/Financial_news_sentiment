from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from math import atanh, erf, sqrt

from sqlalchemy import func
from sqlalchemy.orm import Session

from app import notable_people as notable_people_mod
from app.config import get_settings
from app.models import ProcessedItem, RawItem, SectorCorrelation, TimeSeriesSentiment
from app.sectors import resolve_sector

# Minimum number of shared, non-empty time buckets before a correlation is
# trusted. Correlations over fewer points are highly unstable, so below this we
# report ``None`` (insufficient data) instead of a misleading number.
MIN_OVERLAP = 5
# Two-sided p-value threshold for flagging a correlation as significant.
SIGNIFICANCE = 0.05


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


def _recency_factor(
    published_at: datetime | None,
    now: datetime | None,
    halflife_hours: float,
) -> float:
    """Exponential decay in [0,1] by article age. Unknown age -> neutral 0.5."""
    if published_at is None:
        return 0.5
    reference = now or datetime.utcnow()
    pub = published_at
    if pub.tzinfo is not None:
        pub = pub.astimezone(timezone.utc).replace(tzinfo=None)
    if reference.tzinfo is not None:
        reference = reference.astimezone(timezone.utc).replace(tzinfo=None)
    age_hours = max(0.0, (reference - pub).total_seconds() / 3600.0)
    if halflife_hours <= 0:
        return 1.0
    return _clamp_0_1(0.5 ** (age_hours / halflife_hours))


def impact_component_breakdown(
    source_type: str,
    engagement_metrics: dict | None,
    relevance_score: float,
    *,
    sentiment_score: float = 0.0,
    model_confidence: float = 0.0,
    published_at: datetime | None = None,
    now: datetime | None = None,
    notable_boost: float = 0.0,
    recency_halflife_hours: float | None = None,
) -> dict[str, float]:
    """Causal, publication-time impact score (no future prices).

    Components (weights sum to 1.0): reliability 0.30, engagement 0.20,
    relevance 0.20, recency 0.10, magnitude 0.15, notable 0.05.
    All new args are keyword-only with safe defaults so existing 3-arg callers
    keep working unchanged.
    """
    if recency_halflife_hours is None:
        recency_halflife_hours = get_settings().impact_recency_halflife_hours

    reliability = source_reliability_score(source_type)
    engagement = normalize_engagement(source_type, engagement_metrics)
    relevance = normalize_entity_relevance(relevance_score)
    recency = _recency_factor(published_at, now, recency_halflife_hours)
    magnitude = _clamp_0_1(abs(float(sentiment_score)) * _clamp_0_1(float(model_confidence)))
    notable = _clamp_0_1(float(notable_boost))

    reliability_contrib = 0.30 * reliability
    engagement_contrib = 0.20 * engagement
    relevance_contrib = 0.20 * relevance
    recency_contrib = 0.10 * recency
    magnitude_contrib = 0.15 * magnitude
    notable_contrib = 0.05 * notable

    return {
        "reliability": round(reliability, 6),
        "engagement": round(engagement, 6),
        "relevance": round(relevance, 6),
        "recency": round(recency, 6),
        "magnitude": round(magnitude, 6),
        "notable": round(notable, 6),
        "reliability_contribution": round(reliability_contrib, 6),
        "engagement_contribution": round(engagement_contrib, 6),
        "relevance_contribution": round(relevance_contrib, 6),
        "recency_contribution": round(recency_contrib, 6),
        "magnitude_contribution": round(magnitude_contrib, 6),
        "notable_contribution": round(notable_contrib, 6),
        "impact_score": round(
            _clamp_0_1(
                reliability_contrib
                + engagement_contrib
                + relevance_contrib
                + recency_contrib
                + magnitude_contrib
                + notable_contrib
            ),
            6,
        ),
    }


def compute_impact_score(
    source_type: str,
    engagement_metrics: dict | None,
    relevance_score: float,
    **kwargs: object,
) -> float:
    breakdown = impact_component_breakdown(
        source_type, engagement_metrics, relevance_score, **kwargs
    )
    return float(breakdown["impact_score"])


def _model_confidence_value(model_confidence: object) -> float:
    """Defensively read the sentiment model confidence from the JSON blob."""
    if isinstance(model_confidence, dict):
        try:
            return float(model_confidence.get("confidence", 0.0) or 0.0)
        except (TypeError, ValueError):
            return 0.0
    return 0.0


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
    # Filter/order by PUBLICATION time so the timeline reflects news dates, not the
    # batch-processing date (items processed today shouldn't pile into one bucket).
    return (
        db_session.query(ProcessedItem, RawItem)
        .join(RawItem, ProcessedItem.raw_item_id == RawItem.id)
        .filter(func.lower(ProcessedItem.company) == normalized_company)
        .filter(RawItem.published_at.isnot(None))
        .filter(RawItem.published_at >= since)
        .order_by(RawItem.published_at.asc())
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
            sentiment_score=processed.sentiment_score,
            model_confidence=_model_confidence_value(processed.model_confidence),
            published_at=raw.published_at,
            notable_boost=notable_people_mod.notable_boost(processed.notable_people or []),
        )

        key = _bucket_start(raw.published_at, bucket=bucket)
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
        people = processed.notable_people or []
        mc = processed.model_confidence if isinstance(processed.model_confidence, dict) else {}
        low_conf = bool((mc.get("comparison") or {}).get("low_confidence")) if mc else False
        sent_conf = round(_model_confidence_value(processed.model_confidence), 4)
        breakdown = impact_component_breakdown(
            source_type=raw.source_type,
            engagement_metrics=raw.engagement_metrics,
            relevance_score=processed.relevance_score,
            sentiment_score=processed.sentiment_score,
            model_confidence=_model_confidence_value(processed.model_confidence),
            published_at=raw.published_at,
            notable_boost=notable_people_mod.notable_boost(people),
        )

        ranked.append(
            {
                "processed_id": str(processed.id),
                "raw_item_id": str(processed.raw_item_id),
                "company": processed.company,
                "title": raw.title,
                "summary": processed.summary,
                "summary_source": (processed.pipeline_flags or {}).get("summary", {}).get("source")
                if isinstance(processed.pipeline_flags, dict)
                else None,
                "source_type": raw.source_type,
                "source_name": raw.source_name,
                "url": raw.url,
                "published_at": raw.published_at.isoformat() if raw.published_at else None,
                "processed_at": processed.processed_at.isoformat(),
                "sentiment_label": processed.sentiment_label,
                "sentiment_score": float(processed.sentiment_score),
                "sentiment_confidence": sent_conf,
                "sentiment_source": mc.get("final_source"),
                "sentiment_low_confidence": low_conf,
                "relevance_score": float(processed.relevance_score),
                "notable_people": people,
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


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + erf(x / sqrt(2.0)))


def _pearson_pvalue(r: float, n: int) -> float | None:
    """Approximate two-sided p-value for a Pearson r via the Fisher z-transform.

    Pure-stdlib (no scipy). Requires n > 3 for the standard error to exist.
    """
    if n is None or n < 4:
        return None
    r = max(-0.999999, min(0.999999, r))
    z = atanh(r)
    se = 1.0 / sqrt(n - 3)
    p = 2.0 * (1.0 - _norm_cdf(abs(z) / se))
    return round(max(0.0, min(1.0, p)), 6)


def _pairwise_complete(
    va: dict[datetime, float],
    vb: dict[datetime, float],
) -> tuple[list[float], list[float], list[datetime]]:
    """Align two sparse series on the buckets where BOTH have a real value."""
    shared = sorted(set(va) & set(vb))
    return [va[k] for k in shared], [vb[k] for k in shared], shared


def _shift_keys(
    series: dict[datetime, float],
    lag: int,
    bucket: str,
) -> dict[datetime, float]:
    """Shift bucket timestamps back by ``lag`` buckets (for lead/lag alignment)."""
    if lag == 0:
        return series
    delta = timedelta(hours=lag) if bucket == "hour" else timedelta(days=lag)
    return {key - delta: value for key, value in series.items()}


def _pair_stats(
    map_a: dict[datetime, float],
    map_b: dict[datetime, float],
    method: str,
) -> dict[str, object]:
    """Correlation + confidence between two sparse sector series.

    Returns ``value=None`` when fewer than ``MIN_OVERLAP`` buckets overlap.
    """
    xs, ys, shared = _pairwise_complete(map_a, map_b)
    n = len(shared)
    if n < MIN_OVERLAP:
        return {"value": None, "n": n, "p_value": None, "significant": False}

    corr = _correlation(xs, ys, method)
    p_value = _pearson_pvalue(corr, n)
    return {
        "value": corr,
        "n": n,
        "p_value": p_value,
        "significant": p_value is not None and p_value < SIGNIFICANCE,
    }


def _best_lag(
    map_a: dict[datetime, float],
    map_b: dict[datetime, float],
    method: str,
    max_lag: int,
    bucket: str,
) -> dict[str, object]:
    """Lag (in buckets) maximizing |correlation|, evaluated pairwise-complete.

    Positive lag => sector A leads sector B. Lags whose overlap is below
    ``MIN_OVERLAP`` are skipped; the chosen lag reports its own n and p-value.
    """
    best: dict[str, object] | None = None

    for lag in range(-max_lag, max_lag + 1):
        xs, ys, shared = _pairwise_complete(map_a, _shift_keys(map_b, lag, bucket))
        if len(shared) < MIN_OVERLAP:
            continue

        corr = _correlation(xs, ys, method)
        if best is None or abs(corr) > abs(float(best["correlation"])):
            best = {
                "lag": lag,
                "correlation": round(corr, 6),
                "n": len(shared),
                "p_value": _pearson_pvalue(corr, len(shared)),
            }

    if best is None:
        return {"lag": 0, "correlation": None, "n": 0, "p_value": None}
    return best


def _is_current_schema(correlation_matrix: object) -> bool:
    """True only for snapshots written with the confidence-aware schema."""
    if not isinstance(correlation_matrix, list) or not correlation_matrix:
        return False
    first = correlation_matrix[0]
    return isinstance(first, dict) and "value_meta" in first


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

        # Only serve a cached snapshot if it carries the current (confidence-aware)
        # schema; older snapshots hold the pre-fix trivial ~1.0 matrices, so fall
        # through and recompute instead.
        if existing and _is_current_schema(existing.correlation_matrix):
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

    # Index the sentiment time-series by PUBLICATION time (when the news happened),
    # not processed_at (when our batch enriched it) — otherwise freshly-enriched
    # items all collapse into one "today" bucket and sectors never overlap.
    since = datetime.utcnow() - timedelta(days=window_days)
    rows = (
        db_session.query(ProcessedItem, RawItem)
        .join(RawItem, ProcessedItem.raw_item_id == RawItem.id)
        .filter(RawItem.published_at.isnot(None))
        .filter(RawItem.published_at >= since)
        .order_by(RawItem.published_at.asc())
        .all()
    )

    sector_buckets: dict[str, dict[datetime, list[dict[str, float]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    sector_company_counts: dict[str, set[str]] = defaultdict(set)
    company_sector_memo: dict[str, str] = {}

    for processed, raw in rows:
        # Derive the sector from the company the article is about (real taxonomy),
        # NOT the legacy hardcoded raw.sector_tags. Resolve each distinct company
        # once per request.
        company = processed.company
        sector = company_sector_memo.get(company)
        if sector is None:
            sector = resolve_sector(db_session, company)
            company_sector_memo[company] = sector

        point_bucket = _bucket_start(raw.published_at, bucket)
        impact = compute_impact_score(
            source_type=raw.source_type,
            engagement_metrics=raw.engagement_metrics,
            relevance_score=processed.relevance_score,
            sentiment_score=processed.sentiment_score,
            model_confidence=_model_confidence_value(processed.model_confidence),
            published_at=raw.published_at,
            notable_boost=notable_people_mod.notable_boost(processed.notable_people or []),
        )
        sentiment = float(processed.sentiment_score)

        sector_buckets[sector][point_bucket].append(
            {
                "sentiment": sentiment,
                "impact": impact,
            }
        )
        sector_company_counts[sector].add(company)

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
    # Sparse {bucket_point: weighted_sentiment} per sector — ONLY buckets with
    # real observations. Correlations use these (no zero-fill); the dense
    # display series below keeps zeros purely for charting.
    sector_maps: dict[str, dict[datetime, float]] = {}

    for sector in sorted(sector_buckets.keys()):
        points = []
        sparse: dict[datetime, float] = {}
        total_items = 0

        for bucket_point in all_bucket_points:
            entries = sector_buckets[sector].get(bucket_point, [])
            if entries:
                total_items += len(entries)
                impact_sum = sum(e["impact"] for e in entries)
                if impact_sum > 0:
                    weighted_sentiment = (
                        sum(e["sentiment"] * e["impact"] for e in entries) / impact_sum
                    )
                else:
                    weighted_sentiment = sum(e["sentiment"] for e in entries) / len(entries)
                sparse[bucket_point] = round(float(weighted_sentiment), 6)
            else:
                weighted_sentiment = 0.0

            points.append(
                {
                    "bucket_start": bucket_point.isoformat(),
                    "weighted_sentiment": round(float(weighted_sentiment), 6),
                    "item_count": len(entries),
                }
            )

        sector_maps[sector] = sparse
        avg_sentiment = sum(sparse.values()) / len(sparse) if sparse else 0.0
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
        values: list[float | None] = []
        value_meta: list[dict[str, object]] = []
        for sector_b in sector_names:
            if sector_a == sector_b:
                stats = {
                    "value": 1.0,
                    "n": len(sector_maps[sector_a]),
                    "p_value": 0.0,
                    "significant": True,
                }
            else:
                stats = _pair_stats(sector_maps[sector_a], sector_maps[sector_b], method)
            values.append(stats["value"])
            value_meta.append(
                {"n": stats["n"], "p_value": stats["p_value"], "significant": stats["significant"]}
            )
        correlation_matrix.append(
            {"sector": sector_a, "values": values, "value_meta": value_meta}
        )

    for index, sector_a in enumerate(sector_names):
        for sector_b in sector_names[index + 1 :]:
            result = _best_lag(
                sector_maps[sector_a],
                sector_maps[sector_b],
                method,
                max_lag,
                bucket,
            )
            lag = int(result["lag"])
            corr = result["correlation"]
            p_value = result["p_value"]

            if corr is None or lag == 0:
                leader = "none"
                follower = "none"
            elif lag > 0:
                leader = sector_a
                follower = sector_b
            else:
                leader = sector_b
                follower = sector_a

            lead_lag.append(
                {
                    "sector_a": sector_a,
                    "sector_b": sector_b,
                    "best_lag": lag,
                    "correlation": corr,
                    "n": result["n"],
                    "p_value": p_value,
                    "significant": p_value is not None and p_value < SIGNIFICANCE,
                    "leader": leader,
                    "follower": follower,
                }
            )

    lead_lag.sort(key=lambda row: abs(float(row["correlation"] or 0.0)), reverse=True)

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