import hashlib
import logging
from datetime import datetime
from typing import Optional

import feedparser

from app.connectors.base import Connector, IngestedItem

log = logging.getLogger(__name__)


class RSSConnector(Connector):
    """Connector for RSS feeds (finance news sources)"""

    source_type = "rss"

    # Curated finance RSS feeds
    DEFAULT_FEEDS = {
        "BBC News Business": "http://feeds.bbc.co.uk/news/business/rss.xml",
        "Reuters Business": "https://feeds.reuters.com/reuters/businessNews",
        "CNBC Top News": "https://www.cnbc.com/id/100003114/device/rss/rss.html",
        "Financial Times Markets": "https://feeds.ft.com/markets",
        "Yahoo Finance": "https://feeds.finance.yahoo.com/rss/2.0/headline",
    }

    def __init__(self, custom_feeds: Optional[dict[str, str]] = None):
        self.source_name = "RSS Feeds"
        self.feeds = custom_feeds or self.DEFAULT_FEEDS

    def validate_config(self) -> bool:
        """RSS feeds are always available (no API key needed)"""
        return True

    def fetch(
        self,
        company: str,
        sectors: Optional[list[str]] = None,
        limit: int = 10,
    ) -> list[IngestedItem]:
        """Fetch from curated RSS feeds and filter by company relevance"""
        items = []
        company_lower = company.lower()
        company_terms = company_lower.split()  # e.g., "Apple Inc" -> ["apple", "inc"]

        for feed_name, feed_url in self.feeds.items():
            if len(items) >= limit:
                break

            try:
                parsed = feedparser.parse(feed_url)
                log.info(f"Fetched {feed_name}: {len(parsed.entries)} entries")

                for entry in parsed.entries:
                    if len(items) >= limit:
                        break

                    title = entry.get("title", "")
                    description = entry.get("summary", entry.get("description", ""))
                    link = entry.get("link", "")

                    # Simple relevance check: does company name appear in title or description?
                    text_to_check = f"{title} {description}".lower()
                    is_relevant = any(term in text_to_check for term in company_terms)

                    if not is_relevant:
                        continue

                    # Parse published date
                    published_at = None
                    if hasattr(entry, "published_parsed") and entry.published_parsed:
                        published_at = datetime(*entry.published_parsed[:6])

                    # Extract author
                    author = entry.get("author", None)

                    # Compute content hash for deduplication
                    content_hash = hashlib.sha256(
                        f"{title}{description}".encode()
                    ).hexdigest()

                    item = IngestedItem(
                        source_type=self.source_type,
                        source_name=feed_name,
                        author=author,
                        title=title,
                        content=description,
                        url=link,
                        published_at=published_at,
                        engagement_metrics=None,
                        company_candidates=[company],
                        sector_tags=sectors or ["Finance"],
                        raw_payload={"content_hash": content_hash},
                    )
                    items.append(item)

            except Exception as exc:
                log.warning(f"Failed to parse {feed_name}: {exc}")
                continue

        return items
