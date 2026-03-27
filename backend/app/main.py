from datetime import datetime, timezone

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse

from .config import get_settings
from .db import check_db_health

settings = get_settings()
app = FastAPI(title=settings.app_name, version=settings.api_version)


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
def ingest_run(authorization: str | None = Header(default=None)) -> dict[str, str]:
    expected_token = settings.ingest_token

    if not expected_token:
        raise HTTPException(status_code=503, detail="ingestion token not configured")

    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")

    token = authorization.removeprefix("Bearer ").strip()
    if token != expected_token:
        raise HTTPException(status_code=401, detail="invalid token")

    # Milestone 0 placeholder: worker pipeline is wired in later milestones.
    return {
        "status": "accepted",
        "message": "ingestion trigger received",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
