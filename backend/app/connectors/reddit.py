import hashlib
import logging
from datetime import datetime
from typing import Optional

from app.connectors.base import Connector, IngestedItem

log = logging.getLogger(__name__)


class RedditConnector(Connector):
    """Connector for Reddit posts and comments"""

    source_type = "reddit"

    SUBREDDITS = [
        "stocks",
        "investing",
        "wallstreetbets",
        "finance",
        "investing",
    ]

    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        user_agent: Optional[str] = None,
    ):
        self.source_name = "Reddit"
        self.client_id = client_id
        self.client_secret = client_secret
        self.user_agent = user_agent or "financial-news-aggregator/1.0"
        self.reddit = None
        self._init_reddit()

    def _init_reddit(self):
        """Initialize PRAW Reddit client if credentials available"""
        try:
            import praw

            if self.client_id and self.client_secret:
                self.reddit = praw.Reddit(
                    client_id=self.client_id,
                    client_secret=self.client_secret,
                    user_agent=self.user_agent,
                )
                log.info("Reddit client initialized")
        except ImportError:
            log.warning("praw library not installed; Reddit connector disabled")
        except Exception as exc:
            log.warning(f"Failed to initialize Reddit client: {exc}")

    def validate_config(self) -> bool:
        """Validate Reddit credentials are configured"""
        if not self.reddit:
            log.warning(
                "Reddit client not configured. "
                "Set REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET environment variables."
            )
            return False
        return True

    def fetch(
        self,
        company: str,
        sectors: Optional[list[str]] = None,
        limit: int = 10,
    ) -> list[IngestedItem]:
        """Fetch posts and comments from target subreddits filtered by company"""
        if not self.validate_config():
            log.error("Reddit not configured; skipping")
            return []

        items = []
        company_lower = company.lower()

        try:
            for subreddit_name in self.SUBREDDITS:
                if len(items) >= limit:
                    break

                try:
                    subreddit = self.reddit.subreddit(subreddit_name)

                    # Fetch top/hot posts
                    for post in subreddit.hot(limit=min(50, limit)):
                        if len(items) >= limit:
                            break

                        # Check if post mentions company
                        text_to_check = f"{post.title} {post.selftext}".lower()
                        if company_lower not in text_to_check:
                            continue

                        item = IngestedItem(
                            source_type=self.source_type,
                            source_name=f"Reddit - r/{subreddit_name}",
                            author=post.author.name if post.author else "Unknown",
                            title=post.title,
                            content=post.selftext,
                            url=post.url,
                            published_at=datetime.fromtimestamp(post.created_utc),
                            engagement_metrics={
                                "upvotes": post.score,
                                "comments": post.num_comments,
                            },
                            company_candidates=[company],
                            sector_tags=["Technology", "Finance"],
                            language="en",
                            raw_payload={
                                "post_id": post.id,
                                "subreddit": subreddit_name,
                                "content_hash": hashlib.sha256(
                                    f"{post.title}{post.selftext}{post.url}".encode()
                                ).hexdigest(),
                            },
                        )
                        items.append(item)

                except Exception as exc:
                    log.warning(f"Failed to fetch from r/{subreddit_name}: {exc}")
                    continue

            log.info(f"Fetched {len(items)} posts from Reddit for {company}")
        except Exception as exc:
            log.error(f"Reddit fetch failed: {exc}")

        return items
