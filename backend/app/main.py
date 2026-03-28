from datetime import datetime, timezone
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from .config import get_settings
from .db import check_db_health, get_engine, init_db
from .connectors.rss import RSSConnector
from .connectors.newsapi import NewsAPIConnector
from .connectors.reddit import RedditConnector
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
    - sources: Comma-separated list of sources (rss, newsapi, reddit)
    """
    expected_token = settings.ingest_token

    if not expected_token:
        raise HTTPException(status_code=503, detail="ingestion token not configured")

    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")

    token = authorization.removeprefix("Bearer ").strip()
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
