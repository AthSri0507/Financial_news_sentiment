"""External market-data sector resolution.

Resolves a company to its official sector via an explicit three-stage pipeline:

    normalized name  ->  ticker symbol  ->  official sector  ->  GICS label

This is deliberately NOT a "classify arbitrary company text -> sector" guess,
which is unreliable for ambiguous names (Reliance, TCS, SBI, L&T, Infosys,
Adani). Known-ambiguous names use a curated ticker seed; everything else falls
back to the provider's name search. Any failure returns ``None`` so the caller
can fall back to "Uncategorized"; this module never raises into a request path.

The provider is pluggable via ``settings.sector_api_provider`` (default
``yfinance``); a keyed provider (FMP/Finnhub) can be added without touching
callers.
"""

import logging

from .config import get_settings

log = logging.getLogger(__name__)


# Curated ticker seed for known-ambiguous names so resolution never depends on
# fuzzy name search. Symbols use yfinance conventions (".NS" for NSE listings).
# Keyed by the canonical company name produced by sectors.normalize_company.
TICKER_SEED: dict[str, str] = {
    "Apple": "AAPL",
    "Microsoft": "MSFT",
    "Google": "GOOGL",
    "Amazon": "AMZN",
    "Tesla": "TSLA",
    "Meta": "META",
    "Nvidia": "NVDA",
    "Intel": "INTC",
    "Reliance": "RELIANCE.NS",
    "TCS": "TCS.NS",
    "Infosys": "INFY.NS",
    "Wipro": "WIPRO.NS",
    "HDFC Bank": "HDFCBANK.NS",
    "ICICI Bank": "ICICIBANK.NS",
    "State Bank of India": "SBIN.NS",
    "Bharti Airtel": "BHARTIARTL.NS",
    "Larsen & Toubro": "LT.NS",
    "ITC": "ITC.NS",
    "Hindustan Unilever": "HINDUNILVR.NS",
    "Bajaj Finance": "BAJFINANCE.NS",
    "Adani Enterprises": "ADANIENT.NS",
    "Sun Pharma": "SUNPHARMA.NS",
    # Tata group constituents
    "Tata Motors": "TATAMOTORS.NS",
    "Tata Steel": "TATASTEEL.NS",
    "Tata Power": "TATAPOWER.NS",
    "Tata Consumer Products": "TATACONSUM.NS",
    "Titan": "TITAN.NS",
    # Adani group constituents
    "Adani Green Energy": "ADANIGREEN.NS",
    "Adani Power": "ADANIPOWER.NS",
    "Adani Ports": "ADANIPORTS.NS",
    "Adani Total Gas": "ATGL.NS",
    "Adani Energy Solutions": "ADANIENSOL.NS",
}

# Yahoo/Morningstar sector names -> the 11 official GICS sector labels we use.
YAHOO_TO_GICS: dict[str, str] = {
    "technology": "Information Technology",
    "financial services": "Financials",
    "financial": "Financials",
    "healthcare": "Health Care",
    "consumer cyclical": "Consumer Discretionary",
    "consumer defensive": "Consumer Staples",
    "communication services": "Communication Services",
    "energy": "Energy",
    "industrials": "Industrials",
    "basic materials": "Materials",
    "real estate": "Real Estate",
    "utilities": "Utilities",
}


def _map_to_gics(provider_sector: str | None) -> str | None:
    if not provider_sector:
        return None
    return YAHOO_TO_GICS.get(provider_sector.strip().lower())


def resolve_ticker(company: str) -> str | None:
    """Stage 1: canonical company name -> ticker symbol.

    Prefers the curated seed; otherwise asks the provider's name search.
    """
    seeded = TICKER_SEED.get(company)
    if seeded:
        return seeded

    provider = get_settings().sector_api_provider.lower()
    if provider == "yfinance":
        return _yf_search_ticker(company)

    log.warning("Unknown sector_api_provider %r; cannot resolve ticker", provider)
    return None


def fetch_sector_by_ticker(ticker: str) -> str | None:
    """Stage 2: ticker symbol -> official sector (mapped onto GICS labels)."""
    provider = get_settings().sector_api_provider.lower()
    if provider == "yfinance":
        return _map_to_gics(_yf_sector_for_ticker(ticker))

    log.warning("Unknown sector_api_provider %r; cannot fetch sector", provider)
    return None


def resolve(company: str) -> tuple[str | None, str | None]:
    """Orchestrate stage 1 -> stage 2. Returns (ticker, gics_sector).

    Either element may be ``None`` on failure. Never raises.
    """
    try:
        ticker = resolve_ticker(company)
        if not ticker:
            return None, None
        return ticker, fetch_sector_by_ticker(ticker)
    except Exception as exc:  # pragma: no cover - defensive, network dependent
        log.warning("Sector resolution failed for %r: %s", company, exc)
        return None, None


# --- yfinance provider implementation -------------------------------------

def _yf_search_ticker(company: str) -> str | None:
    try:
        import yfinance as yf

        quotes = getattr(yf.Search(company, max_results=5), "quotes", None) or []
        for quote in quotes:
            symbol = quote.get("symbol")
            quote_type = (quote.get("quoteType") or "").upper()
            if symbol and quote_type == "EQUITY":
                return symbol
        # Fall back to the first symbol regardless of type.
        for quote in quotes:
            if quote.get("symbol"):
                return quote["symbol"]
    except Exception as exc:  # pragma: no cover - network dependent
        log.warning("yfinance ticker search failed for %r: %s", company, exc)
    return None


def _yf_sector_for_ticker(ticker: str) -> str | None:
    try:
        import yfinance as yf

        info = yf.Ticker(ticker).info or {}
        return info.get("sector")
    except Exception as exc:  # pragma: no cover - network dependent
        log.warning("yfinance sector lookup failed for %r: %s", ticker, exc)
    return None
