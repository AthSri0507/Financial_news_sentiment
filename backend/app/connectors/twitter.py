import logging
from typing import Optional
from datetime import datetime

import tweepy

from app.connectors.base import Connector, IngestedItem

log = logging.getLogger(__name__)


class TwitterConnector(Connector):
    """Connector for X (formerly Twitter) API v2"""

    source_type = "x"

    # Company aliases for broader search coverage
    COMPANY_ALIASES = {
        "Apple": ["Apple", "AAPL", "$AAPL"],
        "Microsoft": ["Microsoft", "MSFT", "$MSFT"],
        "Google": ["Google", "GOOGL", "$GOOGL", "Alphabet"],
        "Amazon": ["Amazon", "AMZN", "$AMZN"],
        "Tesla": ["Tesla", "TSLA", "$TSLA"],
        "Meta": ["Meta", "Facebook", "META", "$META"],
        "Nvidia": ["Nvidia", "NVDA", "$NVDA"],
        "Intel": ["Intel", "INTC", "$INTC"],
    }

    def __init__(self, bearer_token: Optional[str] = None):
        self.source_name = "X"
        self.bearer_token = bearer_token
        self.client = None
        if bearer_token:
            self.client = tweepy.Client(bearer_token=bearer_token)

    def validate_config(self) -> bool:
        """Validate bearer token is configured"""
        if not self.bearer_token:
            log.warning("X (Twitter) bearer token not configured; connector will not work")
            return False
        return True

    def fetch(
        self,
        company: str,
        sectors: Optional[list[str]] = None,
        limit: int = 10,
    ) -> list[IngestedItem]:
        """Fetch tweets from X filtered by company"""
        if not self.validate_config() or not self.client:
            log.error("X (Twitter) not configured; skipping")
            return []

        items = []

        # Build search query with company aliases
        search_terms = [company]
        if company in self.COMPANY_ALIASES:
            search_terms.extend(self.COMPANY_ALIASES[company])

        # Build query: company mentions, exclude retweets, English language
        query = " OR ".join(f'"{term}"' for term in search_terms[:3])  # Limit to avoid query length issues
        query += " -is:retweet lang:en"

        log.info(f"Searching X for: {query}")

        try:
            # Fetch tweets with engagement metrics
            tweets = self.client.search_recent_tweets(
                query=query,
                max_results=min(limit, 100),  # X API max is 100
                tweet_fields=["created_at", "author_id", "public_metrics", "lang"],
                expansions=["author_id"],
                user_fields=["username", "verified"],
            )

            if not tweets.data:
                log.info(f"No tweets found on X for {company}")
                return []

            # Create user lookup dict
            users = {user.id: user for user in (tweets.includes["users"] or [])}

            for tweet in tweets.data:
                item = self._parse_tweet(tweet, users, company)
                if item:
                    items.append(item)
                    if len(items) >= limit:
                        break

            log.info(f"Fetched {len(items)} tweets from X for {company}")
            return items

        except tweepy.TweepyException as exc:
            log.error(f"X API error: {exc}")
            return []
        except Exception as exc:
            log.error(f"Error fetching tweets from X: {exc}")
            return []

    def _parse_tweet(self, tweet: dict, users: dict, company: str) -> Optional[IngestedItem]:
        """Parse tweet data into IngestedItem"""
        try:
            author = None
            if tweet.author_id in users:
                author = f"@{users[tweet.author_id].username}"

            metrics = tweet.public_metrics or {}

            item = IngestedItem(
                source_type=self.source_type,
                source_name=self.source_name,
                author=author,
                title=tweet.text[:200],  # Use first 200 chars as title
                content=tweet.text,
                url=f"https://twitter.com/i/web/status/{tweet.id}",
                published_at=tweet.created_at,
                engagement_metrics={
                    "likes": metrics.get("like_count", 0),
                    "retweets": metrics.get("retweet_count", 0),
                    "replies": metrics.get("reply_count", 0),
                    "quotes": metrics.get("quote_count", 0),
                },
                company_candidates=[company],
                sector_tags=["Technology", "Finance"],
                language=tweet.lang or "en",
                raw_payload={
                    "tweet_id": tweet.id,
                    "author_id": tweet.author_id,
                    "metrics": metrics,
                },
            )

            return item

        except Exception as exc:
            log.error(f"Error parsing tweet: {exc}")
            return None
