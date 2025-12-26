"""
RSS News Fetcher Module.

This module provides functionality to fetch and parse RSS feeds
from news sources.
"""

import logging
from typing import Any
import feedparser
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from .types import Article

logger = logging.getLogger(__name__)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((ConnectionError, TimeoutError)),
    reraise=True
)
def fetch_rss_articles(url: str, source: str) -> list[dict[str, Any]]:
    """Fetch articles from an RSS feed URL with retry logic.

    Args:
        url: The URL of the RSS feed to fetch articles from.
        source: The name of the news source for attribution.

    Returns:
        List of article dictionaries, each containing:
        - title: Article title
        - url: Article URL
        - summary: Article summary/description
        - source: News source name

    Raises:
        Exception: If an unexpected error occurs during fetching.
    """
    try:
        logger.info("Fetching data from URL: %s", url)
        # timeout parameter is not directly supported by feedparser.parse but strictly speaking
        # it handles http connections internally. For stricter control standard requests could be used
        # but for now we stick to feedparser.
        feed = feedparser.parse(url)

        if feed.status != 200:
            logger.warning("Non-200 status code received: %s", feed.status)
            # Find a way to trigger retry if it's a transient error, 
            # though feedparser might not raise for status codes.
            # We treat non-200 as empty result generally, unless we want to retry on 5xx.
            return []

        articles_data = []
        for entry in feed.entries:
            # Create Pydantic model to validating data
            article = Article(
                title=entry.get("title", "No title"),
                url=entry.get("link", "No URL"),
                summary=entry.get("summary", "No summary"),
                source=source
            )
            # Dump to dict to maintain compatibility with existing pipeline
            articles_data.append(article.model_dump())

        logger.info(
            "Successfully extracted %d articles from %s",
            len(articles_data),
            source
        )
        return articles_data

    except Exception as e:
        logger.error(
            "Unexpected error fetching from %s: %s",
            url,
            e,
            exc_info=True
        )
        raise


    