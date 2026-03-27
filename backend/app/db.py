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
        _engine = create_engine(settings.database_url, pool_pre_ping=True)

    return _engine


def check_db_health() -> tuple[bool, str]:
    engine = get_engine()
    if engine is None:
        return True, "database not configured"

    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return True, "database ok"
    except Exception as exc:  # pragma: no cover - defensive for infra failures
        return False, f"database unavailable: {exc}"
