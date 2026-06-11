"""Company -> sector taxonomy and layered resolver.

Replaces the old hardcoded ``sector_tags=["Technology", "Finance"]`` tagging
that made every article belong to the same one or two "sectors" (so cross-sector
correlations were trivially ~1.0). A real GICS sector is derived from the
*company* an article is about.

Resolution is layered so it's fast/deterministic for well-known names but still
returns a real sector for any company a user enters:

    1. curated map (offline, instant; also an override that wins over the API)
    2. ``company_sectors`` DB cache (resolved once, reused, manually correctable)
    3. external market-data API (``market_data.resolve``) -> persisted to cache
    4. fallback "Uncategorized" (not cached, so it can be retried later)

Connectors use the offline :func:`curated_sector` only (no DB/network during
bulk ingestion); analytics is the source of truth and uses :func:`resolve_sector`.
"""

import re
from datetime import datetime

from sqlalchemy.orm import Session

from . import market_data
from .models import CompanySector

UNCATEGORIZED = "Uncategorized"

# The 11 official GICS sectors plus the explicit "unknown" bucket.
ALL_SECTORS: list[str] = [
    "Communication Services",
    "Consumer Discretionary",
    "Consumer Staples",
    "Energy",
    "Financials",
    "Health Care",
    "Industrials",
    "Information Technology",
    "Materials",
    "Real Estate",
    "Utilities",
    UNCATEGORIZED,
]

# Curated seed/override: canonical company name -> one primary GICS sector.
COMPANY_SECTORS: dict[str, str] = {
    "Apple": "Information Technology",
    "Microsoft": "Information Technology",
    "Nvidia": "Information Technology",
    "Intel": "Information Technology",
    "TCS": "Information Technology",
    "Infosys": "Information Technology",
    "Wipro": "Information Technology",
    "Google": "Communication Services",
    "Meta": "Communication Services",
    "Bharti Airtel": "Communication Services",
    "Amazon": "Consumer Discretionary",
    "Tesla": "Consumer Discretionary",
    "HDFC Bank": "Financials",
    "ICICI Bank": "Financials",
    "State Bank of India": "Financials",
    "Bajaj Finance": "Financials",
    "ITC": "Consumer Staples",
    "Hindustan Unilever": "Consumer Staples",
    "Reliance": "Energy",
    "Adani Enterprises": "Energy",
    "Sun Pharma": "Health Care",
    "Larsen & Toubro": "Industrials",
    # Tata group constituents (GICS by primary listed business)
    "Tata Motors": "Consumer Discretionary",
    "Tata Steel": "Materials",
    "Tata Power": "Utilities",
    "Tata Consumer Products": "Consumer Staples",
    "Titan": "Consumer Discretionary",
    # Adani group constituents
    "Adani Green Energy": "Utilities",
    "Adani Power": "Utilities",
    "Adani Ports": "Industrials",
    "Adani Total Gas": "Utilities",
    "Adani Energy Solutions": "Utilities",
}

# Preferred (most liquid / highest news-volume) representative per sector for the
# default ingest basket. Any supported sector without an entry falls back to the
# first company found in COMPANY_SECTORS, so this need not be exhaustive.
SECTOR_REPRESENTATIVE: dict[str, str] = {
    "Information Technology": "Apple",
    "Communication Services": "Google",
    "Consumer Discretionary": "Amazon",
    "Financials": "HDFC Bank",
    "Consumer Staples": "ITC",
    "Energy": "Reliance",
    "Health Care": "Sun Pharma",
    "Industrials": "Larsen & Toubro",
}

# Lowercased alias/ticker -> canonical company name.
ALIASES: dict[str, str] = {
    "aapl": "Apple",
    "apple inc": "Apple",
    "msft": "Microsoft",
    "microsoft corp": "Microsoft",
    "googl": "Google",
    "google llc": "Google",
    "alphabet": "Google",
    "amzn": "Amazon",
    "amazon.com": "Amazon",
    "tsla": "Tesla",
    "tesla inc": "Tesla",
    "fb": "Meta",
    "facebook": "Meta",
    "meta platforms": "Meta",
    "nvda": "Nvidia",
    "nvidia corp": "Nvidia",
    "intc": "Intel",
    "intel corp": "Intel",
    "reliance industries": "Reliance",
    "ril": "Reliance",
    "tata consultancy services": "TCS",
    "infy": "Infosys",
    "infosys ltd": "Infosys",
    "infosys limited": "Infosys",
    "wit": "Wipro",
    "wipro ltd": "Wipro",
    "hdfc": "HDFC Bank",
    "hdfc bank ltd": "HDFC Bank",
    "icici": "ICICI Bank",
    "icici bank ltd": "ICICI Bank",
    "sbi": "State Bank of India",
    "sbin": "State Bank of India",
    "airtel": "Bharti Airtel",
    "l&t": "Larsen & Toubro",
    "lt": "Larsen & Toubro",
    "itc ltd": "ITC",
    "hul": "Hindustan Unilever",
    "hindustan unilever ltd": "Hindustan Unilever",
    "bajaj finance ltd": "Bajaj Finance",
    "adani enterprises ltd": "Adani Enterprises",
    "sun pharmaceutical": "Sun Pharma",
    # Tata group variants
    "tata motors ltd": "Tata Motors",
    "tatamotors": "Tata Motors",
    "tata steel ltd": "Tata Steel",
    "tatasteel": "Tata Steel",
    "tata power": "Tata Power",
    "tata power company": "Tata Power",
    "tatapower": "Tata Power",
    "tata consumer": "Tata Consumer Products",
    "tata consumer products ltd": "Tata Consumer Products",
    "tataconsum": "Tata Consumer Products",
    "titan company": "Titan",
    "titan ltd": "Titan",
    # Adani group variants
    "adani green": "Adani Green Energy",
    "adani green energy ltd": "Adani Green Energy",
    "adanigreen": "Adani Green Energy",
    "adani power ltd": "Adani Power",
    "adanipower": "Adani Power",
    "adani ports": "Adani Ports",
    "adani ports and special economic zone": "Adani Ports",
    "adaniports": "Adani Ports",
    "adani total gas": "Adani Total Gas",
    "adani total gas ltd": "Adani Total Gas",
    "atgl": "Adani Total Gas",
    "adani transmission": "Adani Energy Solutions",
    "adani energy solutions": "Adani Energy Solutions",
    "adaniensol": "Adani Energy Solutions",
}


def normalize_company(name: str) -> str:
    """Trim and resolve aliases/tickers to a canonical company name."""
    cleaned = (name or "").strip()
    if not cleaned:
        return cleaned
    return ALIASES.get(cleaned.lower(), cleaned)


def curated_sector(name: str) -> str | None:
    """Offline, curated-map-only lookup (no DB/network). Used by connectors."""
    return COMPANY_SECTORS.get(normalize_company(name))


def curated_tags(name: str) -> list[str]:
    """Offline sector tags for ingestion-time tagging (analytics overrides these)."""
    sector = curated_sector(name)
    return [sector] if sector else [UNCATEGORIZED]


def default_basket() -> list[str]:
    """One liquid representative company per currently-supported sector.

    Derived from COMPANY_SECTORS (grouped by sector) so it auto-evolves as
    sectors/coverage change — never a frozen list. SECTOR_REPRESENTATIVE picks
    the most liquid name per sector; any sector without an override falls back to
    the first company found for it. Quota-friendly default for batch ingest;
    the full universe is opt-in.
    """
    by_sector: dict[str, list[str]] = {}
    for company, sector in COMPANY_SECTORS.items():
        by_sector.setdefault(sector, []).append(company)

    basket: list[str] = []
    for sector in sorted(by_sector):
        preferred = SECTOR_REPRESENTATIVE.get(sector)
        if preferred in by_sector[sector]:
            basket.append(preferred)
        else:
            basket.append(sorted(by_sector[sector])[0])
    return basket


# Lazily-built index of seeded company names/aliases -> ticker, longest-first.
_ARTICLE_TICKER_INDEX: list[tuple[str, str]] | None = None


def _ticker_index() -> list[tuple[str, str]]:
    global _ARTICLE_TICKER_INDEX
    if _ARTICLE_TICKER_INDEX is None:
        seed = market_data.TICKER_SEED
        names: dict[str, str] = {}
        for canonical, ticker in seed.items():
            names[canonical.lower()] = ticker
        for alias, canonical in ALIASES.items():
            if canonical in seed:
                names.setdefault(alias.lower(), seed[canonical])
        # Longest names first so "adani power" wins over a bare "adani".
        _ARTICLE_TICKER_INDEX = sorted(names.items(), key=lambda kv: len(kv[0]), reverse=True)
    return _ARTICLE_TICKER_INDEX


def ticker_for_article(title: str, company: str) -> str | None:
    """Best ticker for an article: the specific seeded company NAMED in the headline,
    else the stored/search company, else None.

    Avoids the umbrella problem where every "Adani" article shared one fuzzy ticker:
    "Adani Power shares gain 3%" -> ADANIPOWER.NS, "Adani Enterprises, ..." -> ADANIENT.NS.
    """
    text = (title or "").lower()
    if text:
        best: tuple[tuple[int, int], str] | None = None  # ((start, -len), ticker)
        for name, ticker in _ticker_index():
            m = re.search(rf"\b{re.escape(name)}\b", text)
            if m:
                key = (m.start(), -len(name))
                if best is None or key < best[0]:
                    best = (key, ticker)
        if best is not None:
            return best[1]
    return market_data.TICKER_SEED.get(normalize_company(company))


def resolve_sector(db: Session, name: str) -> str:
    """Full layered resolver: curated -> cache -> external API -> Uncategorized.

    Persists ticker + sector + source to ``company_sectors`` on first resolve.
    """
    canonical = normalize_company(name)
    if not canonical:
        return UNCATEGORIZED

    # 1. Curated override (also seed the cache so it's traceable/correctable).
    seeded = COMPANY_SECTORS.get(canonical)
    if seeded:
        _upsert_cache(db, canonical, market_data.TICKER_SEED.get(canonical), seeded, "curated")
        return seeded

    # 2. DB cache.
    cached = (
        db.query(CompanySector)
        .filter(CompanySector.company == canonical)
        .first()
    )
    if cached:
        return cached.sector

    # 3. External market-data API.
    ticker, sector = market_data.resolve(canonical)
    if sector:
        _upsert_cache(db, canonical, ticker, sector, "api")
        return sector

    # 4. Fallback (not cached so it can be retried on a later run).
    return UNCATEGORIZED


def sectors_for_company(db: Session, name: str) -> list[str]:
    """Thin wrapper returning a list (one primary sector per company)."""
    return [resolve_sector(db, name)]


def _upsert_cache(
    db: Session,
    company: str,
    ticker: str | None,
    sector: str,
    source: str,
) -> None:
    existing = (
        db.query(CompanySector)
        .filter(CompanySector.company == company)
        .first()
    )
    if existing:
        existing.ticker = ticker
        existing.sector = sector
        existing.source = source
        existing.resolved_at = datetime.utcnow()
    else:
        db.add(
            CompanySector(
                company=company,
                ticker=ticker,
                sector=sector,
                source=source,
            )
        )
    db.commit()
