import feedparser
import logging

logger = logging.getLogger(__name__)


def fetch_rss_articles(url: str) -> list[dict]:

    articles_data = []
    try:
        logger.info(f"Fetching data from URL: {url}")
        feed = feedparser.parse(url)

        if feed.status == 200:
            for entry in feed.entries:
                entry_title = entry.get("title", "no title")
                entry_url = entry.get("link", "no url")
                entry_summary = entry.get("summary", "no summary")
                articles_data.append(
                    {"title": entry_title, "url": entry_url, "summary": entry_summary}
                )

            logger.info(
                f"Successfully extracted {len(articles_data)} articles from {url}."
            )
            return articles_data
        else:
            logger.info(f"Status code not 200 {feed.status}")

    except Exception as e:
        logger.error(
            f"An unexpected error occurred during fetching {url}: {e}", exc_info=True
        )
        return []
