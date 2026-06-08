import logging
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from . import quota
from .analytics import get_ranked_items, get_sector_insights, get_timeline, run_query
from .config import get_settings
from .connectors.marketaux import MarketauxConnector
from .connectors.newsapi import NewsAPIConnector
from .connectors.reddit import RedditConnector
from .connectors.rss import RSSConnector
from .db import check_db_health, get_engine, init_db
from .energy import estimate_energy
from .enrichment import run_enrichment_pipeline
from .ingestion import store_raw_items
from .models import CompanyFetchState, PriceValidation
from .price_validation import (
    agreement_history,
    ensure_validations,
    record_agreement_snapshot,
    run_validations,
    validation_agreement,
)
from .sectors import COMPANY_SECTORS, default_basket

log = logging.getLogger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    log.info("Initializing application")
    init_db()
    yield
    # Shutdown
    log.info("Shutting down application")


app = FastAPI(title=settings.app_name, version=settings.api_version, lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _require_token(authorization: str | None) -> None:
    """Validate the bearer/raw ingestion token; raise HTTPException on failure."""
    expected_token = settings.ingest_token
    if not expected_token:
        raise HTTPException(status_code=503, detail="ingestion token not configured")

    auth_value = (authorization or "").strip()
    if not auth_value:
        raise HTTPException(status_code=401, detail="missing authorization token")

    # Accept both "Bearer <token>" and raw token for easier manual testing.
    token = auth_value[7:].strip() if auth_value.lower().startswith("bearer ") else auth_value
    if not token:
        raise HTTPException(status_code=401, detail="missing authorization token")
    if token != expected_token:
        raise HTTPException(status_code=401, detail="invalid token")


# Named multi-company group baskets (conglomerates spanning several sectors).
GROUP_BASKETS: dict[str, list[str]] = {
    "tata": [
        "TCS",
        "Tata Motors",
        "Tata Steel",
        "Tata Power",
        "Tata Consumer Products",
        "Titan",
    ],
    "adani": [
        "Adani Enterprises",
        "Adani Green Energy",
        "Adani Power",
        "Adani Ports",
        "Adani Total Gas",
        "Adani Energy Solutions",
    ],
}


def _resolve_companies(companies: str | None) -> list[str]:
    """Resolve the ?companies param to a list.

    None/"default" -> the quota-friendly per-sector basket; "all" -> the full
    curated universe; "tata"/"adani" (alone or comma-mixed) -> that group's
    constituents; otherwise a comma-separated explicit list.
    """
    value = (companies or "").strip().lower()
    if not value or value == "default":
        return default_basket()
    if value == "all":
        return list(COMPANY_SECTORS.keys())

    resolved: list[str] = []
    seen: set[str] = set()
    for token in (t.strip() for t in companies.split(",") if t.strip()):
        expanded = GROUP_BASKETS.get(token.lower(), [token])
        for company in expanded:
            if company not in seen:
                seen.add(company)
                resolved.append(company)
    return resolved


def _ingest_one(
    db_session: Session,
    company: str,
    sources: str,
    limit: int,
    from_date: str | None = None,
    to_date: str | None = None,
) -> int:
    """Ingest one company across the requested sources (date-aware where supported)."""
    total = 0
    source_list = [s.strip().lower() for s in sources.split(",") if s.strip()]

    if "rss" in source_list:
        try:
            total += store_raw_items(db_session, RSSConnector().fetch(company=company, limit=limit))
        except Exception as exc:
            log.error("RSS ingestion failed for %s: %s", company, exc)

    if "newsapi" in source_list and getattr(settings, "newsapi_key", None):
        try:
            connector = NewsAPIConnector(api_key=settings.newsapi_key)
            total += store_raw_items(
                db_session,
                connector.fetch(company=company, limit=limit, from_date=from_date, to_date=to_date),
            )
        except Exception as exc:
            log.error("NewsAPI ingestion failed for %s: %s", company, exc)

    if "marketaux" in source_list and getattr(settings, "marketaux_api_key", None):
        try:
            connector = MarketauxConnector(api_key=settings.marketaux_api_key)
            total += store_raw_items(
                db_session,
                connector.fetch(
                    company=company,
                    limit=limit,
                    published_after=from_date,
                    published_before=to_date,
                ),
            )
        except Exception as exc:
            log.error("Marketaux ingestion failed for %s: %s", company, exc)

    if "reddit" in source_list:
        reddit_id = getattr(settings, "reddit_client_id", None)
        reddit_secret = getattr(settings, "reddit_client_secret", None)
        if reddit_id and reddit_secret:
            try:
                connector = RedditConnector(client_id=reddit_id, client_secret=reddit_secret)
                total += store_raw_items(db_session, connector.fetch(company=company, limit=limit))
            except Exception as exc:
                log.error("Reddit ingestion failed for %s: %s", company, exc)

    return total


def _enrich_company(db_session: Session, company: str) -> int:
    """Run the enrichment pipeline for one company; return processed count."""
    result = run_enrichment_pipeline(
        db_session=db_session,
        company=company,
        limit=settings.nlp_max_items_per_run,
        min_relevance=settings.nlp_min_relevance,
        max_text_chars=settings.nlp_max_text_chars,
        prefer_finbert=settings.nlp_prefer_finbert,
        finbert_min_confidence=settings.nlp_finbert_min_confidence,
        hf_api_key=settings.huggingface_api_key,
        prefer_hf_ner=settings.nlp_prefer_hf_ner,
        ner_model_id=settings.nlp_ner_model_id,
        prefer_hf_summary=settings.nlp_prefer_hf_summary,
        summary_model_id=settings.nlp_summary_model_id,
        summary_min_chars=settings.nlp_summary_min_chars,
        summary_max_input_chars=settings.nlp_summary_max_input_chars,
    )
    return int(result.get("processed", 0) or 0)


# Per-company in-process locks so concurrent requests in one worker coalesce
# their refresh instead of each spending quota (cross-worker is handled by the
# claim-before-fetch on CompanyFetchState.last_attempted_at).
_company_locks: dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()


def _company_lock(company: str) -> threading.Lock:
    key = company.strip().lower()
    with _locks_guard:
        lock = _company_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _company_locks[key] = lock
        return lock


def _do_refresh(db_session: Session, company: str) -> tuple[int, list[str], bool]:
    """Quota-aware fetch+enrich for one company. Returns (new_items, sources_used, ok)."""
    limit = settings.freshness_on_view_limit
    new_items = 0
    used: list[str] = []
    ok = False

    # Always RSS (free) — guarantees fresh items even when paid quota is gone.
    try:
        n = store_raw_items(db_session, RSSConnector().fetch(company=company, limit=limit))
        new_items += n
        used.append("rss")
        ok = True
    except Exception as exc:
        log.error("Freshness RSS fetch failed for %s: %s", company, exc)

    # Paid sources only while daily budget remains.
    if getattr(settings, "newsapi_key", None) and quota.remaining(db_session, "newsapi") > 0:
        try:
            connector = NewsAPIConnector(api_key=settings.newsapi_key)
            items = connector.fetch(company=company, limit=limit)
            quota.record(db_session, "newsapi", 1)
            if getattr(connector, "rate_limited", False):
                quota.mark_exhausted(db_session, "newsapi")
            new_items += store_raw_items(db_session, items)
            used.append("newsapi")
            ok = True
        except Exception as exc:
            log.error("Freshness NewsAPI fetch failed for %s: %s", company, exc)

    marketaux_key = getattr(settings, "marketaux_api_key", None)
    if marketaux_key and quota.remaining(db_session, "marketaux") > 0:
        try:
            connector = MarketauxConnector(api_key=marketaux_key)
            items = connector.fetch(company=company, limit=limit)
            quota.record(db_session, "marketaux", 1)
            if getattr(connector, "rate_limited", False):
                quota.mark_exhausted(db_session, "marketaux")
            new_items += store_raw_items(db_session, items)
            used.append("marketaux")
            ok = True
        except Exception as exc:
            log.error("Freshness Marketaux fetch failed for %s: %s", company, exc)

    if new_items > 0:
        try:
            _enrich_company(db_session, company)
        except Exception as exc:
            log.error("Freshness enrichment failed for %s: %s", company, exc)

    return new_items, used, ok


def _refresh_if_stale(db_session: Session, company: str) -> dict[str, object]:
    """Blocking, cooldown-gated, dedup'd on-view refresh. Never raises."""
    if not settings.freshness_on_view_enabled:
        return {"refreshed": False, "reason": "disabled", "new_items": 0, "sources_used": []}

    cooldown = timedelta(minutes=settings.freshness_cooldown_minutes)
    now = datetime.utcnow()
    lock = _company_lock(company)

    # Same-worker dedupe: if another request is already refreshing this company,
    # don't pile on — serve cached.
    if not lock.acquire(blocking=False):
        return {"refreshed": False, "reason": "in_progress", "new_items": 0, "sources_used": []}
    try:
        state = (
            db_session.query(CompanyFetchState)
            .filter(CompanyFetchState.company == company)
            .first()
        )
        if (
            state is not None
            and state.last_attempted_at is not None
            and (now - state.last_attempted_at) < cooldown
        ):
            return {
                "refreshed": False,
                "reason": "cooldown",
                "new_items": 0,
                "sources_used": [],
                "last_attempted_at": state.last_attempted_at,
            }

        # Claim before fetching (cross-worker guard): set last_attempted_at now.
        if state is None:
            state = CompanyFetchState(company=company)
            db_session.add(state)
        state.last_attempted_at = now
        db_session.commit()

        new_items, sources_used, ok = _do_refresh(db_session, company)

        state.last_item_count = new_items
        if ok:
            state.last_success_at = datetime.utcnow()
        db_session.commit()

        return {
            "refreshed": True,
            "ok": ok,
            "new_items": new_items,
            "sources_used": sources_used,
            "last_attempted_at": state.last_attempted_at,
        }
    except Exception as exc:  # pragma: no cover - defensive, never break a read
        db_session.rollback()
        log.error("Freshness refresh failed for %s: %s", company, exc)
        return {"refreshed": False, "reason": "error", "new_items": 0, "sources_used": []}
    finally:
        lock.release()


def _build_freshness(
    db_session: Session,
    company: str,
    refresh: dict[str, object],
    newest_published_at: str | None,
) -> dict[str, object]:
    """Assemble the freshness block (view_state + transparency fields) for /query."""
    state = (
        db_session.query(CompanyFetchState)
        .filter(CompanyFetchState.company == company)
        .first()
    )
    last_attempted = state.last_attempted_at if state else None
    minutes_since = (
        round((datetime.utcnow() - last_attempted).total_seconds() / 60.0, 1)
        if last_attempted
        else None
    )

    if refresh.get("refreshed"):
        if not refresh.get("ok"):
            view_state = "unavailable"
        elif int(refresh.get("new_items", 0) or 0) > 0:
            view_state = "updated"
        else:
            view_state = "checked"
    else:
        view_state = "unavailable" if refresh.get("reason") == "error" else "cached"

    return {
        "view_state": view_state,
        "new_items": int(refresh.get("new_items", 0) or 0),
        "sources_used": refresh.get("sources_used", []),
        "last_fetched_at": last_attempted.isoformat() if last_attempted else None,
        "minutes_since_fetch": minutes_since,
        "newest_published_at": newest_published_at,
        "quota": {
            "newsapi_remaining": quota.remaining(db_session, "newsapi"),
            "marketaux_remaining": quota.remaining(db_session, "marketaux"),
        },
    }


def _reaction_state(row: PriceValidation | None) -> str:
    """Map a primary-window validation row to a single user-facing state."""
    if row is None:
        return "pending"
    status = row.validation_status
    if status == "validated":
        if row.moved_market and row.price_change_pct is not None:
            return "positive" if row.price_change_pct > 0 else "negative"
        return "none"
    if status in ("no_data", "market_closed"):
        return "undetermined"
    return "pending"  # window not yet elapsed


def _attach_price_validations(db_session: Session, items: list[dict]) -> list[dict]:
    """Ensure + attach observed market reaction to ranked items (display only).

    Lazily computes the primary window for displayed items so historical articles
    converge to a terminal result instead of indefinite "pending." Kept at the
    endpoint layer so analytics.py stays free of PriceValidation — impact/ranking/
    timeline/correlation never read price data.
    """
    ids = [it.get("processed_id") for it in items if it.get("processed_id")]
    if not ids:
        return items

    ensure_validations(db_session, ids, primary_only=True)

    primary = settings.price_validation_primary_window
    rows = (
        db_session.query(PriceValidation)
        .filter(PriceValidation.processed_item_id.in_(ids))
        .all()
    )
    windows_by_item: dict[str, dict] = {}
    primary_by_item: dict[str, PriceValidation] = {}
    for row in rows:
        key = str(row.processed_item_id)
        windows_by_item.setdefault(key, {})[row.validation_window] = {
            "validation_status": row.validation_status,
            "price_change_pct": row.price_change_pct,
            "moved_market": row.moved_market,
            "validation_confidence": row.validation_confidence,
        }
        if row.validation_window == primary:
            primary_by_item[key] = row

    for item in items:
        key = str(item.get("processed_id"))
        item["price_validation"] = windows_by_item.get(key, {})
        primary_row = primary_by_item.get(key)
        item["market_reaction"] = {
            "state": _reaction_state(primary_row),
            "change_pct": primary_row.price_change_pct if primary_row else None,
            "window": primary,
        }
    return items


@app.get("/health")
def health() -> dict[str, str]:
    return {
        "status": "ok",
        "service": settings.app_name,
        "environment": settings.environment,
        "version": settings.api_version,
    }


@app.get("/health/dependencies")
def health_dependencies() -> JSONResponse:
    db_ok, db_message = check_db_health()

    payload = {
        "status": "ok" if db_ok else "degraded",
        "database": {
            "ok": db_ok,
            "message": db_message,
        },
    }

    return JSONResponse(content=payload, status_code=200 if db_ok else 503)


@app.post("/ingest/run")
def ingest_run(
    authorization: str | None = Header(default=None),
    company: str = "Apple",
    sources: str = "rss,newsapi",
) -> dict[str, str]:
    """
    Run ingestion pipeline for specified company and sources.

    Query parameters:
    - company: Company name to search for (default: "Apple")
    - sources: Comma-separated list of sources (rss, newsapi, marketaux, reddit)
    """
    _require_token(authorization)

    try:
        engine = get_engine()
        if not engine:
            raise HTTPException(status_code=503, detail="database not configured")

        # Initialize database session
        db_session = Session(engine)
        total_stored = 0
        per_source = settings.ingest_per_source_limit

        # Parse requested sources
        source_list = [s.strip().lower() for s in sources.split(",")]
        log.info(f"Ingesting for company '{company}' from sources: {source_list}")

        # RSS Connector (always available)
        if "rss" in source_list:
            try:
                rss_connector = RSSConnector()
                items = rss_connector.fetch(company=company, limit=per_source)
                stored = store_raw_items(db_session, items)
                total_stored += stored
                log.info(f"RSS: {stored} items stored")
            except Exception as exc:
                log.error(f"RSS ingestion failed: {exc}")

        # NewsAPI Connector (requires API key)
        if "newsapi" in source_list:
            try:
                newsapi_key = getattr(settings, "newsapi_key", None)
                if not newsapi_key:
                    log.warning("NewsAPI key not configured; skipping")
                else:
                    newsapi_connector = NewsAPIConnector(api_key=newsapi_key)
                    items = newsapi_connector.fetch(company=company, limit=per_source)
                    stored = store_raw_items(db_session, items)
                    total_stored += stored
                    log.info(f"NewsAPI: {stored} items stored")
            except Exception as exc:
                log.error(f"NewsAPI ingestion failed: {exc}")

        # Marketaux Connector (requires API key)
        if "marketaux" in source_list:
            try:
                marketaux_key = getattr(settings, "marketaux_api_key", None)
                if not marketaux_key:
                    log.warning("Marketaux key not configured; skipping")
                else:
                    marketaux_connector = MarketauxConnector(api_key=marketaux_key)
                    items = marketaux_connector.fetch(company=company, limit=per_source)
                    stored = store_raw_items(db_session, items)
                    total_stored += stored
                    log.info(f"Marketaux: {stored} items stored")
            except Exception as exc:
                log.error(f"Marketaux ingestion failed: {exc}")

        # Reddit Connector (requires credentials)
        if "reddit" in source_list:
            try:
                reddit_id = getattr(settings, "reddit_client_id", None)
                reddit_secret = getattr(settings, "reddit_client_secret", None)
                if not reddit_id or not reddit_secret:
                    log.warning("Reddit credentials not configured; skipping")
                else:
                    reddit_connector = RedditConnector(
                        client_id=reddit_id,
                        client_secret=reddit_secret,
                    )
                    items = reddit_connector.fetch(company=company, limit=per_source)
                    stored = store_raw_items(db_session, items)
                    total_stored += stored
                    log.info(f"Reddit: {stored} items stored")
            except Exception as exc:
                log.error(f"Reddit ingestion failed: {exc}")

        db_session.close()

        return {
            "status": "success",
            "message": f"ingestion completed: {total_stored} items stored",
            "company": company,
            "sources": sources,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as exc:
        log.error(f"Ingestion failed: {exc}")
        raise HTTPException(status_code=500, detail=f"ingestion error: {exc}")


@app.post("/ingest/batch")
def ingest_batch(
    authorization: str | None = Header(default=None),
    companies: str | None = None,
    sources: str = "rss,newsapi,marketaux",
    enrich: bool = True,
) -> dict[str, object]:
    """Ingest a sector-spanning basket of companies (default: one per sector).

    `companies`: omit/"default" -> per-sector basket; "all" -> full universe;
    or a comma-separated explicit list. Optionally enriches each company inline.
    """
    _require_token(authorization)

    engine = get_engine()
    if not engine:
        raise HTTPException(status_code=503, detail="database not configured")

    db_session = Session(engine)
    try:
        company_list = _resolve_companies(companies)
        results: dict[str, dict[str, int]] = {}
        for company in company_list:
            stored = _ingest_one(db_session, company, sources, settings.ingest_per_source_limit)
            enriched = _enrich_company(db_session, company) if enrich else 0
            results[company] = {"stored": stored, "enriched": enriched}

        return {
            "status": "success",
            "companies": len(company_list),
            "results": results,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as exc:
        log.error("Batch ingestion failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"batch ingestion error: {exc}")
    finally:
        db_session.close()


@app.post("/ingest/backfill")
def ingest_backfill(
    authorization: str | None = Header(default=None),
    companies: str | None = None,
    days: int = 30,
    sources: str = "newsapi,marketaux",
    enrich: bool = True,
) -> dict[str, object]:
    """Seed ~`days` of history per company by iterating daily date windows.

    Only NewsAPI/Marketaux honor date ranges (free tiers ~30 days); RSS/Reddit
    cannot truly backfill and are excluded from the default `sources`.
    """
    _require_token(authorization)
    days = max(1, min(days, 30))

    engine = get_engine()
    if not engine:
        raise HTTPException(status_code=503, detail="database not configured")

    db_session = Session(engine)
    try:
        company_list = _resolve_companies(companies)
        now = datetime.now(timezone.utc)
        results: dict[str, dict[str, int]] = {}

        for company in company_list:
            stored = 0
            for day in range(days):
                window_end = now - timedelta(days=day)
                window_start = now - timedelta(days=day + 1)
                stored += _ingest_one(
                    db_session,
                    company,
                    sources,
                    settings.ingest_per_source_limit,
                    from_date=window_start.strftime("%Y-%m-%dT%H:%M:%S"),
                    to_date=window_end.strftime("%Y-%m-%dT%H:%M:%S"),
                )
            enriched = _enrich_company(db_session, company) if enrich else 0
            results[company] = {"stored": stored, "enriched": enriched}

        return {
            "status": "success",
            "companies": len(company_list),
            "days": days,
            "results": results,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as exc:
        log.error("Backfill ingestion failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"backfill ingestion error: {exc}")
    finally:
        db_session.close()


@app.post("/enrich/run")
def enrich_run(
    authorization: str | None = Header(default=None),
    company: str = "Apple",
    limit: int = 30,
    min_relevance: float | None = None,
) -> dict[str, object]:
    """Run milestone-2 NLP enrichment and persist to processed_items."""
    expected_token = settings.ingest_token

    if not expected_token:
        raise HTTPException(status_code=503, detail="ingestion token not configured")

    auth_value = (authorization or "").strip()
    if not auth_value:
        raise HTTPException(status_code=401, detail="missing authorization token")

    if auth_value.lower().startswith("bearer "):
        token = auth_value[7:].strip()
    else:
        token = auth_value

    if token != expected_token:
        raise HTTPException(status_code=401, detail="invalid token")

    engine = get_engine()
    if not engine:
        raise HTTPException(status_code=503, detail="database not configured")

    session = Session(engine)
    try:
        effective_limit = min(limit, settings.nlp_max_items_per_run)
        effective_relevance = (
            settings.nlp_min_relevance if min_relevance is None else min_relevance
        )

        result = run_enrichment_pipeline(
            db_session=session,
            company=company,
            limit=effective_limit,
            min_relevance=effective_relevance,
            max_text_chars=settings.nlp_max_text_chars,
            prefer_finbert=settings.nlp_prefer_finbert,
            finbert_min_confidence=settings.nlp_finbert_min_confidence,
            hf_api_key=settings.huggingface_api_key,
        )

        result["timestamp"] = datetime.now(timezone.utc).isoformat()
        return result
    except Exception as exc:
        log.error("Enrichment failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"enrichment error: {exc}")
    finally:
        session.close()


@app.post("/query")
def query_run(
    company: str = "Apple",
    bucket: str = "day",
    window_days: int = 7,
    item_limit: int = 20,
    recompute_timeline: bool = False,
) -> dict[str, object]:
    """Run impact-scored query response with timeline + ranked items."""
    engine = get_engine()
    if not engine:
        raise HTTPException(status_code=503, detail="database not configured")

    session = Session(engine)
    try:
        result = run_query(
            db_session=session,
            company=company,
            bucket=bucket,
            window_days=window_days,
            item_limit=item_limit,
            recompute_timeline=recompute_timeline,
        )

        # News-driven freshness: on a stale (or cold) view, pull recent news
        # (quota-aware, cooldown-gated, dedup'd), then re-run the query.
        refresh = _refresh_if_stale(session, company)
        if refresh.get("refreshed") and int(refresh.get("new_items", 0) or 0) > 0:
            result = run_query(
                db_session=session,
                company=company,
                bucket=bucket,
                window_days=window_days,
                item_limit=item_limit,
                recompute_timeline=True,
            )

        items = result.get("items") if isinstance(result.get("items"), list) else []
        if items:
            _attach_price_validations(session, items)
        newest = max(
            (it.get("published_at") for it in items if it.get("published_at")),
            default=None,
        )

        result["freshness"] = _build_freshness(session, company, refresh, newest)
        result["timestamp"] = datetime.now(timezone.utc).isoformat()
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        log.error("Query pipeline failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"query error: {exc}")
    finally:
        session.close()


@app.get("/items")
def query_items(
    company: str = "Apple",
    window_days: int = 7,
    limit: int = 20,
) -> dict[str, object]:
    """Return impact-ranked items for a company and time window."""
    engine = get_engine()
    if not engine:
        raise HTTPException(status_code=503, detail="database not configured")

    session = Session(engine)
    try:
        items = get_ranked_items(
            db_session=session,
            company=company,
            window_days=window_days,
            limit=limit,
        )
        _attach_price_validations(session, items)
        return {
            "status": "success",
            "company": company,
            "window_days": window_days,
            "count": len(items),
            "items": items,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as exc:
        log.error("Items endpoint failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"items error: {exc}")
    finally:
        session.close()


@app.post("/validate/run")
def validate_run(
    authorization: str | None = Header(default=None),
    company: str | None = None,
    limit: int = 50,
) -> dict[str, object]:
    """Compute due price-validation windows (post-publication, separate from impact)."""
    _require_token(authorization)

    engine = get_engine()
    if not engine:
        raise HTTPException(status_code=503, detail="database not configured")

    session = Session(engine)
    try:
        result = run_validations(session, company=company, limit=max(1, min(limit, 500)))
        # Persist today's agreement KPI snapshot (idempotent) for the long-term trend.
        if not company:
            result["agreement"] = record_agreement_snapshot(session)
        result["timestamp"] = datetime.now(timezone.utc).isoformat()
        return result
    except Exception as exc:
        log.error("Validation run failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"validation error: {exc}")
    finally:
        session.close()


@app.get("/validation/agreement")
def validation_agreement_endpoint(
    window: str = "1d",
    impact_threshold: float = 0.75,
    window_days: int = 30,
) -> dict[str, object]:
    """Read-only: share of high-impact predictions followed by a real market move."""
    engine = get_engine()
    if not engine:
        raise HTTPException(status_code=503, detail="database not configured")

    session = Session(engine)
    try:
        result = validation_agreement(
            session,
            window=window,
            impact_threshold=impact_threshold,
            window_days=window_days,
        )
        result["timestamp"] = datetime.now(timezone.utc).isoformat()
        return result
    except Exception as exc:
        log.error("Validation agreement failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"agreement error: {exc}")
    finally:
        session.close()


@app.get("/energy")
def energy_endpoint() -> dict[str, object]:
    """Estimated model energy & carbon from stored inference counts (read-only)."""
    engine = get_engine()
    if not engine:
        raise HTTPException(status_code=503, detail="database not configured")

    session = Session(engine)
    try:
        result = estimate_energy(session)
        result["timestamp"] = datetime.now(timezone.utc).isoformat()
        return result
    except Exception as exc:
        log.error("Energy endpoint failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"energy error: {exc}")
    finally:
        session.close()


@app.get("/validation/agreement/history")
def validation_agreement_history_endpoint(
    window: str = "1d",
    days: int = 90,
) -> dict[str, object]:
    """Read-only: the agreement-KPI snapshot series (model quality over time)."""
    engine = get_engine()
    if not engine:
        raise HTTPException(status_code=503, detail="database not configured")

    session = Session(engine)
    try:
        series = agreement_history(session, window=window, days=days)
        return {
            "status": "success",
            "window": window,
            "series": series,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as exc:
        log.error("Agreement history failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"agreement history error: {exc}")
    finally:
        session.close()


@app.get("/timeline")
def query_timeline(
    company: str = "Apple",
    bucket: str = "day",
    window_days: int = 7,
    recompute: bool = False,
) -> dict[str, object]:
    """Return impact-weighted sentiment timeline."""
    engine = get_engine()
    if not engine:
        raise HTTPException(status_code=503, detail="database not configured")

    session = Session(engine)
    try:
        timeline = get_timeline(
            db_session=session,
            company=company,
            bucket=bucket,
            window_days=window_days,
            recompute=recompute,
        )
        return {
            "status": "success",
            "company": company,
            "bucket": bucket,
            "window_days": window_days,
            "count": len(timeline),
            "timeline": timeline,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        log.error("Timeline endpoint failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"timeline error: {exc}")
    finally:
        session.close()


@app.get("/sector-insights")
def query_sector_insights(
    bucket: str = "day",
    window_days: int = 14,
    method: str = "pearson",
    max_lag: int = 3,
    recompute: bool = False,
) -> dict[str, object]:
    """Return cross-sector sentiment series, correlations, and lead/lag results."""
    engine = get_engine()
    if not engine:
        raise HTTPException(status_code=503, detail="database not configured")

    session = Session(engine)
    try:
        result = get_sector_insights(
            db_session=session,
            bucket=bucket,
            window_days=window_days,
            method=method,
            max_lag=max_lag,
            recompute=recompute,
        )
        result["timestamp"] = datetime.now(timezone.utc).isoformat()
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        log.error("Sector insights endpoint failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"sector-insights error: {exc}")
    finally:
        session.close()
