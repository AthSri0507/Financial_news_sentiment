"""Shared daily paid-API call budget.

Both the on-view refresh and the scheduled jobs record paid-source calls here so
their combined usage can't exceed the free-tier limits. RSS is free and never
counted. All helpers are failure-safe: if the DB hiccups, `remaining` returns 0
(fail closed → degrade to RSS-only) and `record`/`mark_exhausted` are no-ops.
"""

import logging
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.config import get_settings
from app.models import ApiUsage

log = logging.getLogger(__name__)

_CAP_SETTING = {
    "newsapi": "newsapi_daily_quota",
    "marketaux": "marketaux_daily_quota",
}


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _cap(provider: str) -> int:
    setting = _CAP_SETTING.get(provider)
    if not setting:
        return 0
    return int(getattr(get_settings(), setting, 0) or 0)


def _row(db_session: Session, provider: str) -> ApiUsage | None:
    return (
        db_session.query(ApiUsage)
        .filter(ApiUsage.provider == provider, ApiUsage.day == _today())
        .first()
    )


def used(db_session: Session, provider: str) -> int:
    try:
        row = _row(db_session, provider)
        return int(row.count) if row else 0
    except Exception as exc:  # pragma: no cover - defensive
        log.warning("quota.used failed for %s: %s", provider, exc)
        return _cap(provider)  # fail closed


def remaining(db_session: Session, provider: str) -> int:
    return max(0, _cap(provider) - used(db_session, provider))


def record(db_session: Session, provider: str, n: int = 1) -> None:
    if provider not in _CAP_SETTING or n <= 0:
        return
    try:
        row = _row(db_session, provider)
        if row is None:
            row = ApiUsage(provider=provider, day=_today(), count=0)
            db_session.add(row)
            db_session.flush()
        row.count = int(row.count) + n
        db_session.commit()
    except Exception as exc:  # pragma: no cover - defensive
        db_session.rollback()
        log.warning("quota.record failed for %s: %s", provider, exc)


def mark_exhausted(db_session: Session, provider: str) -> None:
    """Pin today's usage to the cap (e.g. after a 429) so it's treated as spent."""
    if provider not in _CAP_SETTING:
        return
    try:
        row = _row(db_session, provider)
        if row is None:
            row = ApiUsage(provider=provider, day=_today(), count=0)
            db_session.add(row)
            db_session.flush()
        row.count = max(int(row.count), _cap(provider))
        db_session.commit()
    except Exception as exc:  # pragma: no cover - defensive
        db_session.rollback()
        log.warning("quota.mark_exhausted failed for %s: %s", provider, exc)
