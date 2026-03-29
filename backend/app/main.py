from datetime import datetime, timezone
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from .config import get_settings
from .db import check_db_health, get_engine, init_db
from .connectors.rss import RSSConnector
from .connectors.newsapi import NewsAPIConnector
from .connectors.marketaux import MarketauxConnector
from .connectors.reddit import RedditConnector
from .analytics import get_ranked_items, get_sector_insights, get_timeline, run_query
from .enrichment import run_enrichment_pipeline
from .ingestion import store_raw_items

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


def _ingest_for_company(
    db_session: Session,
    company: str,
    sources: str = "rss,newsapi,marketaux",
) -> int:
    total_stored = 0
    source_list = [s.strip().lower() for s in sources.split(",") if s.strip()]

    if "rss" in source_list:
        try:
            rss_connector = RSSConnector()
            items = rss_connector.fetch(company=company, sectors=["Technology"], limit=10)
            total_stored += store_raw_items(db_session, items)
        except Exception as exc:
            log.error("Autofetch RSS ingestion failed: %s", exc)

    if "newsapi" in source_list:
        try:
            newsapi_key = getattr(settings, "newsapi_key", None)
            if newsapi_key:
                newsapi_connector = NewsAPIConnector(api_key=newsapi_key)
                items = newsapi_connector.fetch(company=company, sectors=["Technology"], limit=10)
                total_stored += store_raw_items(db_session, items)
            else:
                log.warning("Autofetch: NewsAPI key not configured; skipping")
        except Exception as exc:
            log.error("Autofetch NewsAPI ingestion failed: %s", exc)

    if "marketaux" in source_list:
        try:
            marketaux_key = getattr(settings, "marketaux_api_key", None)
            if marketaux_key:
                marketaux_connector = MarketauxConnector(api_key=marketaux_key)
                items = marketaux_connector.fetch(company=company, sectors=["Technology"], limit=10)
                total_stored += store_raw_items(db_session, items)
            else:
                log.warning("Autofetch: Marketaux key not configured; skipping")
        except Exception as exc:
            log.error("Autofetch Marketaux ingestion failed: %s", exc)

    return total_stored


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
    expected_token = settings.ingest_token

    if not expected_token:
        raise HTTPException(status_code=503, detail="ingestion token not configured")

    auth_value = (authorization or "").strip()
    if not auth_value:
        raise HTTPException(status_code=401, detail="missing authorization token")

    # Accept both "Bearer <token>" and raw token for easier manual testing.
    if auth_value.lower().startswith("bearer "):
        token = auth_value[7:].strip()
    else:
        token = auth_value

    if not token:
        raise HTTPException(status_code=401, detail="missing authorization token")

    if token != expected_token:
        raise HTTPException(status_code=401, detail="invalid token")

    try:
        engine = get_engine()
        if not engine:
            raise HTTPException(status_code=503, detail="database not configured")

        # Initialize database session
        db_session = Session(engine)
        total_stored = 0

        # Parse requested sources
        source_list = [s.strip().lower() for s in sources.split(",")]
        log.info(f"Ingesting for company '{company}' from sources: {source_list}")

        # RSS Connector (always available)
        if "rss" in source_list:
            try:
                rss_connector = RSSConnector()
                items = rss_connector.fetch(company=company, sectors=["Technology"], limit=10)
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
                    items = newsapi_connector.fetch(company=company, sectors=["Technology"], limit=10)
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
                    items = marketaux_connector.fetch(company=company, sectors=["Technology"], limit=10)
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
                    items = reddit_connector.fetch(company=company, sectors=["Technology"], limit=10)
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

        autofetch_meta: dict[str, object] = {
            "triggered": False,
            "stored_raw_items": 0,
            "enriched_items": 0,
            "status": "skipped",
        }

        has_data = bool(result.get("timeline_points", 0)) or bool(result.get("items_returned", 0))
        if not has_data:
            autofetch_meta["triggered"] = True
            try:
                stored_count = _ingest_for_company(
                    db_session=session,
                    company=company,
                    sources="rss,newsapi,marketaux",
                )
                enrich_result = run_enrichment_pipeline(
                    db_session=session,
                    company=company,
                    limit=min(100, settings.nlp_max_items_per_run),
                    min_relevance=settings.nlp_min_relevance,
                    max_text_chars=settings.nlp_max_text_chars,
                    prefer_finbert=settings.nlp_prefer_finbert,
                    finbert_min_confidence=settings.nlp_finbert_min_confidence,
                    hf_api_key=settings.huggingface_api_key,
                )
                result = run_query(
                    db_session=session,
                    company=company,
                    bucket=bucket,
                    window_days=window_days,
                    item_limit=item_limit,
                    recompute_timeline=True,
                )
                autofetch_meta["stored_raw_items"] = int(stored_count)
                autofetch_meta["enriched_items"] = int(enrich_result.get("processed_count", 0))
                autofetch_meta["status"] = "completed"
            except Exception as exc:
                log.error("Autofetch on query miss failed: %s", exc)
                autofetch_meta["status"] = "failed"
                autofetch_meta["error"] = str(exc)

        result["autofetch"] = autofetch_meta
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
