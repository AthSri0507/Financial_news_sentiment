import hashlib
import logging
from datetime import datetime
from typing import Optional

import requests

from app.connectors.base import Connector, IngestedItem
from app.utils.retry import retry_with_backoff

log = logging.getLogger(__name__)


class MarketauxConnector(Connector):
    """Connector for Marketaux financial news API."""

    source_type = "marketaux"

    COMPANY_TO_SYMBOL = {
        "Apple": "AAPL",
        "Microsoft": "MSFT",
        "Google": "GOOGL",
        "Amazon": "AMZN",
        "Tesla": "TSLA",
        "Meta": "META",
        "Nvidia": "NVDA",
        "Intel": "INTC",
        "Reliance": "RELIANCE.NSE",
        "Reliance Industries": "RELIANCE.NSE",
        "TCS": "TCS.NSE",
        "Tata Consultancy Services": "TCS.NSE",
        "Infosys": "INFY.NSE",
        "HDFC Bank": "HDFCBANK.NSE",
        "ICICI Bank": "ICICIBANK.NSE",
        "State Bank of India": "SBIN.NSE",
        "SBI": "SBIN.NSE",
        "Bharti Airtel": "BHARTIARTL.NSE",
        "Larsen & Toubro": "LT.NSE",
        "L&T": "LT.NSE",
        "ITC": "ITC.NSE",
        "Hindustan Unilever": "HINDUNILVR.NSE",
        "Bajaj Finance": "BAJFINANCE.NSE",
        "Adani Enterprises": "ADANIENT.NSE",
        "Sun Pharma": "SUNPHARMA.NSE",
        "Wipro": "WIPRO.NSE",
    }

    def __init__(self, api_key: Optional[str] = None):
        self.source_name = "Marketaux"
        self.api_key = api_key
        self.base_url = "https://api.marketaux.com/v1/news/all"

    def validate_config(self) -> bool:
        if not self.api_key:
            log.warning("Marketaux key not configured; connector will not work")
            return False
        return True

    def fetch(
        self,
        company: str,
        sectors: Optional[list[str]] = None,
        limit: int = 10,
    ) -> list[IngestedItem]:
        if not self.validate_config():
            log.error("Marketaux not configured; skipping")
            return []

        symbol_key = next(
            (name for name in self.COMPANY_TO_SYMBOL if name.lower() == company.lower()),
            None,
        )
        symbol = self.COMPANY_TO_SYMBOL.get(symbol_key) if symbol_key else None
        try:
            payload = retry_with_backoff(
                self._fetch_news,
                max_retries=2,
                initial_delay=1.0,
                company=company,
                symbol=symbol,
                limit=limit,
            )
        except Exception as exc:
            log.error("Marketaux fetch failed: %s", exc)
            return []

        items: list[IngestedItem] = []
        for row in payload.get("data", []):
            parsed = self._parse_item(row=row, company=company)
            if parsed:
                items.append(parsed)
                if len(items) >= limit:
                    break

        log.info("Fetched %s articles from Marketaux for %s", len(items), company)
        return items

    def _fetch_news(self, company: str, symbol: Optional[str], limit: int) -> dict:
        params = {
            "api_token": self.api_key,
            "language": "en",
            "limit": min(limit, 50),
            "sort": "published_desc",
            "search": company,
        }
        if symbol:
            params["symbols"] = symbol

        response = requests.get(self.base_url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    def _parse_item(self, row: dict, company: str) -> Optional[IngestedItem]:
        try:
            title = (row.get("title") or "").strip()
            description = (row.get("description") or "").strip()
            url = (row.get("url") or "").strip()
            if not title or not url:
                return None

            published_at = None
            published_str = row.get("published_at")
            if published_str:
                try:
                    published_at = datetime.fromisoformat(published_str.replace("Z", "+00:00"))
                except Exception:
                    published_at = None

            source_name = (row.get("source") or "Marketaux").strip() or "Marketaux"
            content_hash = hashlib.sha256(f"{title}{description}{url}".encode()).hexdigest()

            return IngestedItem(
                source_type=self.source_type,
                source_name=source_name,
                author=row.get("author"),
                title=title,
                content=description or title,
                url=url,
                published_at=published_at,
                engagement_metrics=None,
                company_candidates=[company],
                sector_tags=["Technology", "Finance"],
                language=(row.get("language") or "en"),
                raw_payload={
                    "article": row,
                    "content_hash": content_hash,
                },
            )
        except Exception as exc:
            log.warning("Failed to parse Marketaux item: %s", exc)
            return None