"""
RSS News Fetcher Module.

This module provides functionality to fetch and parse RSS feeds
from news sources.
"""

import logging
from typing import Any

import feedparser

logger = logging.getLogger(__name__)


def fetch_rss_articles(url: str, source: str) -> list[dict[str, Any]]:
    """Fetch articles from an RSS feed URL.

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
        feed = feedparser.parse(url)

        if feed.status != 200:
            logger.warning("Non-200 status code received: %s", feed.status)
            return []

        articles_data = []
        for entry in feed.entries:
            article = {
                "title": entry.get("title", "No title"),
                "url": entry.get("link", "No URL"),
                "summary": entry.get("summary", "No summary"),
                "source": source,
            }
            articles_data.append(article)

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

    