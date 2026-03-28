import hashlib
import logging
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Optional
from xml.etree import ElementTree as ET

import requests

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
                entries = self._read_entries(feed_url)
                log.info(f"Fetched {feed_name}: {len(entries)} entries")

                for entry in entries:
                    if len(items) >= limit:
                        break

                    title = entry.get("title", "")
                    description = entry.get("description", "")
                    link = entry.get("link", "")

                    # Simple relevance check: does company name appear in title or description?
                    text_to_check = f"{title} {description}".lower()
                    is_relevant = any(term in text_to_check for term in company_terms)

                    if not is_relevant:
                        continue

                    published_at = entry.get("published_at")
                    author = entry.get("author")

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

    def _read_entries(self, feed_url: str) -> list[dict]:
        """Parse RSS/Atom entries without external parser dependency."""
        response = requests.get(
            feed_url,
            timeout=10,
            headers={"User-Agent": "financial-news-ingestor/1.0"},
        )
        response.raise_for_status()

        root = ET.fromstring(response.content)
        rss_items = root.findall(".//item")
        atom_entries = root.findall(".//{http://www.w3.org/2005/Atom}entry")

        if rss_items:
            return [self._parse_rss_item(node) for node in rss_items]
        return [self._parse_atom_entry(node) for node in atom_entries]

    def _parse_rss_item(self, node: ET.Element) -> dict:
        title = self._find_text(node, ["title"])
        description = self._find_text(node, ["description", "summary"])
        link = self._find_text(node, ["link"])
        author = self._find_text(node, ["author", "creator"])
        published_text = self._find_text(node, ["pubDate", "published", "updated"])

        return {
            "title": title,
            "description": description,
            "link": link,
            "author": author,
            "published_at": self._parse_date(published_text),
        }

    def _parse_atom_entry(self, node: ET.Element) -> dict:
        atom_ns = "{http://www.w3.org/2005/Atom}"
        title = self._find_text(node, [f"{atom_ns}title", "title"])
        description = self._find_text(node, [f"{atom_ns}summary", f"{atom_ns}content", "summary"])

        link = ""
        link_node = node.find(f"{atom_ns}link") or node.find("link")
        if link_node is not None:
            link = link_node.attrib.get("href") or (link_node.text or "")

        author = ""
        author_node = node.find(f"{atom_ns}author") or node.find("author")
        if author_node is not None:
            name_node = author_node.find(f"{atom_ns}name") or author_node.find("name")
            author = (name_node.text or "") if name_node is not None else (author_node.text or "")

        published_text = self._find_text(node, [f"{atom_ns}published", f"{atom_ns}updated", "published", "updated"])

        return {
            "title": title,
            "description": description,
            "link": link,
            "author": author,
            "published_at": self._parse_date(published_text),
        }

    @staticmethod
    def _find_text(node: ET.Element, tags: list[str]) -> str:
        for tag in tags:
            child = node.find(tag)
            if child is not None and child.text:
                return child.text.strip()
        return ""

    @staticmethod
    def _parse_date(value: str) -> datetime | None:
        if not value:
            return None

        try:
            if value.endswith("Z"):
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            return datetime.fromisoformat(value)
        except Exception:
            pass

        try:
            return parsedate_to_datetime(value)
        except Exception:
            return None
