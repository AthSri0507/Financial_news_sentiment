import logging
from typing import Optional

import requests

from app.connectors.base import Connector, IngestedItem
from app.utils.retry import retry_with_backoff
from datetime import datetime
import hashlib

log = logging.getLogger(__name__)


class NewsAPIConnector(Connector):
    """Connector for NewsAPI.org (global news search)"""

    source_type = "newsapi"

    # Company aliases for broader search coverage
    COMPANY_ALIASES = {
        "Apple": ["AAPL", "Apple Inc"],
        "Microsoft": ["MSFT", "Microsoft Corp"],
        "Google": ["GOOGL", "Google LLC", "Alphabet"],
        "Amazon": ["AMZN", "Amazon.com"],
        "Tesla": ["TSLA", "Tesla Inc"],
        "Meta": ["META", "Facebook", "Meta Platforms"],
        "Nvidia": ["NVDA", "Nvidia Corp"],
        "Intel": ["INTC", "Intel Corp"],
        "Reliance": ["Reliance Industries", "RIL", "RELIANCE"],
        "TCS": ["Tata Consultancy Services", "TCS.NS"],
        "Infosys": ["INFY", "Infosys Ltd", "Infosys Limited"],
        "HDFC Bank": ["HDB", "HDFC", "HDFC Bank Ltd"],
        "ICICI Bank": ["IBN", "ICICI", "ICICI Bank Ltd"],
        "State Bank of India": ["SBI", "SBIN", "SBIN.NS"],
        "Bharti Airtel": ["Airtel", "BHARTIARTL", "BHARTIARTL.NS"],
        "Larsen & Toubro": ["L&T", "LT", "LT.NS"],
        "ITC": ["ITC Ltd", "ITC.NS"],
        "Hindustan Unilever": ["HUL", "Hindustan Unilever Ltd", "HINDUNILVR"],
        "Bajaj Finance": ["BAJFINANCE", "BAJFINANCE.NS"],
        "Adani Enterprises": ["ADANIENT", "ADANIENT.NS"],
        "Sun Pharma": ["Sun Pharmaceutical", "SUNPHARMA", "SUNPHARMA.NS"],
        "Wipro": ["WIT", "WIPRO", "WIPRO.NS"],
    }

    def __init__(self, api_key: Optional[str] = None):
        self.source_name = "NewsAPI"
        self.api_key = api_key
        self.base_url = "https://newsapi.org/v2/everything"

    def validate_config(self) -> bool:
        """Validate API key is configured"""
        if not self.api_key:
            log.warning("NewsAPI key not configured; connector will not work")
            return False
        return True

    def fetch(
        self,
        company: str,
        sectors: Optional[list[str]] = None,
        limit: int = 10,
    ) -> list[IngestedItem]:
        """Fetch articles from NewsAPI filtered by company"""
        if not self.validate_config():
            log.error("NewsAPI not configured; skipping")
            return []

        items = []

        # Build search query with company aliases
        search_terms = [company]
        alias_key = next(
            (name for name in self.COMPANY_ALIASES if name.lower() == company.lower()),
            None,
        )
        if alias_key:
            search_terms.extend(self.COMPANY_ALIASES[alias_key])

        query = " OR ".join(search_terms)
        log.info(f"Searching NewsAPI for: {query}")

        try:
            response = retry_with_backoff(
                self._fetch_articles,
                max_retries=2,
                initial_delay=1.0,
                query=query,
                limit=limit,
            )

            for article in response.get("articles", []):
                item = self._parse_article(article, company)
                if item:
                    items.append(item)
                    if len(items) >= limit:
                        break

            log.info(f"Fetched {len(items)} articles from NewsAPI for {company}")
        except Exception as exc:
            log.error(f"NewsAPI fetch failed: {exc}")

        return items

    def _fetch_articles(self, query: str, limit: int) -> dict:
        """Fetch articles from NewsAPI with retryable HTTP request"""
        params = {
            "q": query,
            "sortBy": "publishedAt",
            "pageSize": min(limit, 100),
            "apiKey": self.api_key,
        }

        response = requests.get(self.base_url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    def _parse_article(self, article: dict, company: str) -> Optional[IngestedItem]:
        """Parse NewsAPI article into IngestedItem"""
        try:
            title = article.get("title", "")
            description = article.get("description", "")
            content = article.get("content", "")
            url = article.get("url", "")
            author = article.get("author")
            source = article.get("source", {})
            source_name = source.get("name", "NewsAPI")

            # Parse published date
            published_at = None
            published_str = article.get("publishedAt")
            if published_str:
                try:
                    published_at = datetime.fromisoformat(published_str.replace("Z", "+00:00"))
                except Exception:
                    pass

            # Extract engagement metrics (views/shares if available)
            engagement_metrics = None
            if article.get("urlToImage"):
                engagement_metrics = {"has_image": True}

            # Compute content hash for deduplication
            content_hash = hashlib.sha256(
                f"{title}{description}{url}".encode()
            ).hexdigest()

            return IngestedItem(
                source_type=self.source_type,
                source_name=source_name,
                author=author,
                title=title,
                content=content or description,
                url=url,
                published_at=published_at,
                engagement_metrics=engagement_metrics,
                company_candidates=[company],
                sector_tags=["Technology", "Finance"],
                language="en",
                raw_payload={
                    "article": article,
                    "content_hash": content_hash,
                },
            )
        except Exception as exc:
            log.warning(f"Failed to parse NewsAPI article: {exc}")
            return None
