import logging
import socket
from time import sleep

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, make_url

from .config import get_settings

_engine: Engine | None = None
log = logging.getLogger(__name__)


def _build_connect_args(database_url: str) -> dict[str, object]:
    """Build psycopg connect args and prefer IPv4 when possible.

    Render/Supabase can fail on IPv6-only resolution in some environments.
    If possible, resolve the hostname to IPv4 and pass hostaddr explicitly.
    """
    # Supabase pooler (PgBouncer transaction mode) can conflict with psycopg
    # server-side prepared statements. Disable auto-prepare for stability.
    connect_args: dict[str, object] = {
        "connect_timeout": 8,
        "prepare_threshold": None,
    }

    try:
        parsed_url = make_url(database_url)
        host = parsed_url.host
        port = parsed_url.port or 5432

        if not host or "hostaddr" in parsed_url.query:
            return connect_args

        ipv4_records = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
        if ipv4_records:
            connect_args["hostaddr"] = ipv4_records[0][4][0]
    except Exception as exc:  # pragma: no cover - network/environment dependent
        log.warning("Could not resolve IPv4 hostaddr for database URL: %s", exc)

    return connect_args


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
            connect_args=_build_connect_args(settings.database_url),
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


def _ensure_columns(engine) -> None:
    """Add columns that ``create_all`` can't add to pre-existing tables.

    ``create_all`` never ALTERs an existing table, so new columns on already-created
    tables (e.g. ``processed_items.notable_people``) need an explicit, idempotent
    ALTER. Postgres supports ``ADD COLUMN IF NOT EXISTS``; SQLite is guarded by a
    PRAGMA check. Fresh databases already have the column via ``create_all`` — this
    is a no-op there.
    """
    # (table, column, type) — JSON works on both Postgres and SQLite.
    required = [("processed_items", "notable_people", "JSON")]
    dialect = engine.dialect.name

    with engine.begin() as conn:
        for table, column, col_type in required:
            try:
                if dialect == "sqlite":
                    rows = conn.exec_driver_sql(f"PRAGMA table_info({table})").fetchall()
                    if any(row[1] == column for row in rows):
                        continue
                    conn.exec_driver_sql(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
                else:
                    conn.exec_driver_sql(
                        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {col_type}"
                    )
            except Exception as exc:  # pragma: no cover - defensive
                log.warning("ensure_columns: could not add %s.%s: %s", table, column, exc)


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
        _ensure_columns(engine)
        log.info("Database initialization complete")
    except Exception as exc:
        log.error(f"Failed to initialize database: {exc}", exc_info=True)
