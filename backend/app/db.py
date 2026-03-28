from time import sleep

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .config import get_settings


_engine: Engine | None = None


def get_engine() -> Engine | None:
    global _engine
    settings = get_settings()

    if not settings.database_url:
        return None

    if _engine is None:
        # Keep dependency health checks responsive even when DB networking is misconfigured.
        _engine = create_engine(
            settings.database_url,
            pool_pre_ping=True,
            connect_args={"connect_timeout": 8},
        )

    return _engine


def check_db_health() -> tuple[bool, str]:
    engine = get_engine()
    if engine is None:
        return True, "database not configured"

    last_error: Exception | None = None

    for attempt in range(2):
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            return True, "database ok"
        except Exception as exc:  # pragma: no cover - defensive for infra failures
            last_error = exc
            if attempt == 0:
                sleep(0.4)

    return False, f"database unavailable: {last_error}"


def init_db():
    """Initialize database tables on application startup"""
    import logging
    log = logging.getLogger(__name__)
    
    try:
        from .models import Base
        engine = get_engine()
        if engine is None:
            log.warning("Database not configured (DATABASE_URL not set)")
            return
        log.info("Creating database tables...")
        Base.metadata.create_all(engine)
        log.info("Database initialization complete")
    except Exception as exc:
        log.error(f"Failed to initialize database: {exc}", exc_info=True)
