"""Post-publication price validation (observational, separate from impact).

For each processed article we capture a baseline price at publication and the
price after configurable windows (+1h, +4h, +1 trading day), then record whether
the market actually moved. This is evidence only: nothing here is read by impact
scoring, ranking, timeline, or correlation. Look-ahead is prevented by only
computing a window once it has actually elapsed.

`1d` is the primary signal; `1h`/`4h` are best-effort (yfinance intraday coverage
is inconsistent) and degrade to `no_data` independently without blocking `1d`.
"""

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from app.config import get_settings
from app.models import AgreementSnapshot, PriceValidation, ProcessedItem, RawItem
from app.sectors import ticker_for_article

log = logging.getLogger(__name__)

WINDOW_DELTAS: dict[str, timedelta] = {
    "1h": timedelta(hours=1),
    "4h": timedelta(hours=4),
    "1d": timedelta(days=1),
}
# yfinance interval per window (intraday for short windows, daily for 1d).
WINDOW_INTERVAL: dict[str, str] = {"1h": "60m", "4h": "60m", "1d": "1d"}


def _to_naive_utc(dt: datetime) -> datetime:
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _parse_windows(raw: str) -> list[str]:
    return [w.strip() for w in (raw or "").split(",") if w.strip() in WINDOW_DELTAS]


def _price_at_or_before(bars: list[tuple[datetime, float]], ts: datetime) -> float | None:
    candidate = None
    for when, price in bars:
        if when <= ts:
            candidate = price
        else:
            break
    return candidate


def _price_at_or_after(bars: list[tuple[datetime, float]], ts: datetime) -> float | None:
    for when, price in bars:
        if when >= ts:
            return price
    return None


def _fetch_bars(
    ticker: str,
    start: datetime,
    end: datetime,
    interval: str,
) -> list[tuple[datetime, float]]:
    """Fetch (timestamp, close) bars from yfinance. Failure-safe (returns [])."""
    try:
        import yfinance as yf

        history = yf.Ticker(ticker).history(start=start, end=end, interval=interval)
        bars: list[tuple[datetime, float]] = []
        for idx, row in history.iterrows():
            when = idx.to_pydatetime() if hasattr(idx, "to_pydatetime") else idx
            bars.append((_to_naive_utc(when), float(row["Close"])))
        return bars
    except Exception as exc:  # pragma: no cover - network dependent
        log.warning("yfinance history failed for %s: %s", ticker, exc)
        return []


def _validate_window(
    row: PriceValidation,
    ticker: str | None,
    published_at: datetime,
    target: datetime,
    window: str,
    threshold_pct: float,
) -> str:
    """Compute one window's reaction into `row`; return the resulting status."""
    if not ticker:
        row.validation_status = "no_data"
        return "no_data"

    interval = WINDOW_INTERVAL[window]
    bars = sorted(
        _fetch_bars(ticker, published_at - timedelta(days=2), target + timedelta(days=3), interval)
    )
    baseline = _price_at_or_before(bars, published_at) or _price_at_or_after(bars, published_at)
    window_price = _price_at_or_after(bars, target)

    if baseline is None or window_price is None or baseline == 0:
        row.validation_status = "no_data"
        return "no_data"

    change_pct = round((window_price - baseline) / baseline * 100.0, 4)
    row.ticker = ticker
    row.baseline_price = round(baseline, 6)
    row.window_price = round(window_price, 6)
    row.price_change_pct = change_pct
    row.moved_market = abs(change_pct) >= threshold_pct
    row.validation_confidence = 1.0
    row.price_validated = True
    row.validation_status = "validated"
    row.validated_at = datetime.utcnow()
    return "validated"


def _compute_window(
    db_session: Session,
    processed,
    ticker: str | None,
    published: datetime,
    window: str,
    reference: datetime,
    threshold: float,
    grace: timedelta,
    existing: dict[str, PriceValidation],
) -> str:
    """Get-or-create a window row and resolve its state (with no-data grace)."""
    target = published + WINDOW_DELTAS[window]
    row = existing.get(window)
    if row is None:
        row = PriceValidation(
            processed_item_id=processed.id,
            company=processed.company,
            ticker=ticker,
            validation_window=window,
            window_target_at=target,
            validation_status="pending",
        )
        db_session.add(row)
        existing[window] = row

    # Heal stale rows: if the resolved ticker changed (e.g. an old fuzzy/umbrella
    # ticker), re-open the row so it's recomputed against the correct ticker.
    if row.ticker != ticker:
        row.ticker = ticker
        row.price_validated = False
        row.validation_status = "pending"
        row.baseline_price = None
        row.window_price = None
        row.price_change_pct = None
        row.moved_market = None
        row.validation_confidence = None
        row.validated_at = None

    if row.price_validated:
        return row.validation_status

    # Look-ahead guard: never validate a window that hasn't elapsed.
    if reference < target:
        row.validation_status = "pending"
        return "pending"

    status = _validate_window(row, ticker, published, target, window, threshold)
    # Converge to a terminal result: after the grace period, stop retrying a
    # window that still has no price data so it reads as "unable to determine"
    # rather than retrying forever.
    if status == "no_data" and reference >= target + grace:
        row.price_validated = True
    return status


def _existing_rows(db_session: Session, processed_id) -> dict[str, PriceValidation]:
    return {
        r.validation_window: r
        for r in db_session.query(PriceValidation)
        .filter(PriceValidation.processed_item_id == processed_id)
        .all()
    }


def run_validations(
    db_session: Session,
    company: str | None = None,
    limit: int = 50,
    now: datetime | None = None,
) -> dict[str, object]:
    """Compute due price-validation windows for recent processed items (all windows)."""
    settings = get_settings()
    if not settings.price_validation_enabled:
        return {"status": "disabled"}

    windows = _parse_windows(settings.price_validation_windows)
    threshold = settings.price_move_threshold_pct
    grace = timedelta(days=settings.price_validation_no_data_grace_days)
    reference = _to_naive_utc(now or datetime.utcnow())

    query = (
        db_session.query(ProcessedItem, RawItem)
        .join(RawItem, ProcessedItem.raw_item_id == RawItem.id)
        .filter(RawItem.published_at.isnot(None))
    )
    if company:
        query = query.filter(ProcessedItem.company == company)
    rows = query.order_by(RawItem.published_at.desc()).limit(limit).all()

    counts: dict[str, int] = {}

    for processed, raw in rows:
        published = _to_naive_utc(raw.published_at)
        ticker = ticker_for_article(raw.title, processed.company)
        existing = _existing_rows(db_session, processed.id)
        for window in windows:
            status = _compute_window(
                db_session, processed, ticker, published, window,
                reference, threshold, grace, existing,
            )
            counts[status] = counts.get(status, 0) + 1

    db_session.commit()
    return {"status": "success", "counts": counts, "items": len(rows)}


def ensure_validations(
    db_session: Session,
    processed_item_ids: list,
    now: datetime | None = None,
    primary_only: bool = True,
) -> None:
    """Lazily compute reactions for the displayed items so old articles converge.

    Computes the primary window (1d) for the given items whose window has elapsed
    and lack a terminal row, batched per ticker. Failure-safe; never raises.
    """
    settings = get_settings()
    if not settings.price_validation_enabled or not processed_item_ids:
        return

    if primary_only:
        windows = [settings.price_validation_primary_window]
    else:
        windows = _parse_windows(settings.price_validation_windows)
    threshold = settings.price_move_threshold_pct
    grace = timedelta(days=settings.price_validation_no_data_grace_days)
    reference = _to_naive_utc(now or datetime.utcnow())

    try:
        rows = (
            db_session.query(ProcessedItem, RawItem)
            .join(RawItem, ProcessedItem.raw_item_id == RawItem.id)
            .filter(ProcessedItem.id.in_(processed_item_ids))
            .filter(RawItem.published_at.isnot(None))
            .all()
        )
        for processed, raw in rows:
            published = _to_naive_utc(raw.published_at)
            ticker = ticker_for_article(raw.title, processed.company)
            existing = _existing_rows(db_session, processed.id)
            for window in windows:
                _compute_window(
                    db_session, processed, ticker, published, window,
                    reference, threshold, grace, existing,
                )
        db_session.commit()
    except Exception as exc:  # pragma: no cover - defensive, never break a read
        db_session.rollback()
        log.warning("ensure_validations failed: %s", exc)


def validation_agreement(
    db_session: Session,
    window: str = "1d",
    impact_threshold: float = 0.75,
    window_days: int = 30,
) -> dict[str, object]:
    """Read-only: how often high (predicted) impact preceded a real market move.

    Measurement only — recomputes the causal impact at query time and joins it
    with stored validation results. Never alters any score.
    """
    from app.analytics import compute_impact_score  # local import avoids a cycle
    from app.notable_people import notable_boost

    since = datetime.utcnow() - timedelta(days=max(1, min(window_days, 365)))
    rows = (
        db_session.query(ProcessedItem, RawItem, PriceValidation)
        .join(RawItem, ProcessedItem.raw_item_id == RawItem.id)
        .join(PriceValidation, PriceValidation.processed_item_id == ProcessedItem.id)
        .filter(PriceValidation.validation_window == window)
        .filter(PriceValidation.price_validated.is_(True))
        .filter(ProcessedItem.processed_at >= since)
        .all()
    )

    n_high = 0
    n_moved = 0
    for processed, raw, validation in rows:
        impact = compute_impact_score(
            source_type=raw.source_type,
            engagement_metrics=raw.engagement_metrics,
            relevance_score=processed.relevance_score,
            sentiment_score=processed.sentiment_score,
            model_confidence=(processed.model_confidence or {}).get("confidence", 0.0)
            if isinstance(processed.model_confidence, dict)
            else 0.0,
            published_at=raw.published_at,
            notable_boost=notable_boost(processed.notable_people or []),
        )
        if impact >= impact_threshold:
            n_high += 1
            if validation.moved_market:
                n_moved += 1

    agreement = round(n_moved / n_high, 4) if n_high else None
    return {
        "status": "success",
        "window": window,
        "impact_threshold": impact_threshold,
        "n_high_impact": n_high,
        "n_moved": n_moved,
        "agreement_pct": round(agreement * 100, 2) if agreement is not None else None,
    }


def record_agreement_snapshot(
    db_session: Session,
    window: str = "1d",
    impact_threshold: float = 0.75,
    window_days: int = 30,
) -> dict[str, object]:
    """Persist today's agreement KPI as an idempotent daily snapshot (for trend)."""
    result = validation_agreement(
        db_session, window=window, impact_threshold=impact_threshold, window_days=window_days
    )
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        row = (
            db_session.query(AgreementSnapshot)
            .filter(
                AgreementSnapshot.day == day,
                AgreementSnapshot.window == window,
                AgreementSnapshot.impact_threshold == impact_threshold,
            )
            .first()
        )
        if row is None:
            row = AgreementSnapshot(day=day, window=window, impact_threshold=impact_threshold)
            db_session.add(row)
        row.n_high_impact = int(result["n_high_impact"])
        row.n_moved = int(result["n_moved"])
        row.agreement_pct = result["agreement_pct"]
        row.generated_at = datetime.utcnow()
        db_session.commit()
    except Exception as exc:  # pragma: no cover - defensive
        db_session.rollback()
        log.warning("record_agreement_snapshot failed: %s", exc)
    return result


def agreement_history(
    db_session: Session,
    window: str = "1d",
    days: int = 90,
) -> list[dict[str, object]]:
    """Return the agreement-KPI snapshot series (most recent last)."""
    rows = (
        db_session.query(AgreementSnapshot)
        .filter(AgreementSnapshot.window == window)
        .order_by(AgreementSnapshot.day.desc())
        .limit(max(1, min(days, 365)))
        .all()
    )
    series = [
        {
            "day": r.day,
            "agreement_pct": r.agreement_pct,
            "n_high_impact": r.n_high_impact,
            "n_moved": r.n_moved,
            "impact_threshold": r.impact_threshold,
        }
        for r in rows
    ]
    series.reverse()
    return series
