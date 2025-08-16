"""
Fetch rss news
"""
import logging
import feedparser

logger = logging.getLogger(__name__)


def fetch_rss_articles(url: str, source: str) -> list[dict]:
    """
    Fetches articles from an RSS feed URL and returns a list of article data dictionaries.

    Args:
        url (str): The URL of the RSS feed to fetch articles from.
        source (str): The name of the news source for attribution.

    Returns:
        list[dict]: A list of dictionaries, each containing 'title', 'url', 'summary', and 'source' keys
        for each article found in the RSS feed. Returns an empty list if the feed status is not 200 or if an error occurs.

    Logs:
        - Info logs for fetching status and number of articles extracted.
        - Error logs if an exception occurs during fetching.
    """

    articles_data = []
    try:
        logger.info("Fetching data from URL: %s", url)
        feed = feedparser.parse(url)

        if feed.status == 200:
            for entry in feed.entries:
                entry_title = entry.get("title", "no title")
                entry_url = entry.get("link", "no url")
                entry_summary = entry.get("summary", "no summary")
                articles_data.append(
                    {
                        "title": entry_title,
                        "url": entry_url,
                        "summary": entry_summary,
                        "source": source,
                    }
                )

            logger.info(
                "Successfully extracted %d articles from %s.", len(articles_data), url
            )
            return articles_data
        else:
            logger.info("Status code not 200 %s", feed.status)
            return []
    except Exception as e:
        logger.error(
            "An unexpected error occurred during fetching %s: %s", url, e, exc_info=True
        )
        raise
    