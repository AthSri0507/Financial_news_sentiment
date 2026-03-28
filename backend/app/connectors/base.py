from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class IngestedItem:
    """Normalized item ready for storage"""

    source_type: str  # news, rss, reddit, gdelt, x
    source_name: str
    author: Optional[str]
    title: str
    content: Optional[str]
    url: str
    published_at: Optional[datetime]
    engagement_metrics: Optional[dict]
    company_candidates: Optional[list[str]]
    sector_tags: Optional[list[str]]
    language: str = "en"
    raw_payload: dict = None

    def __post_init__(self):
        if self.raw_payload is None:
            self.raw_payload = {}


class Connector(ABC):
    """Base class for all data source connectors"""

    source_type: str
    source_name: str

    @abstractmethod
    def fetch(
        self,
        company: str,
        sectors: Optional[list[str]] = None,
        limit: int = 10,
    ) -> list[IngestedItem]:
        """
        Fetch items from source.

        Args:
            company: company name or ticker
            sectors: optional sector tags for filtering
            limit: max items to return

        Returns:
            List of IngestedItem objects
        """
        pass

    @abstractmethod
    def validate_config(self) -> bool:
        """Validate connector is properly configured"""
        pass
