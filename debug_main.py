"""
Standalone version of the news feed pipeline for debugging without Airflow.
Runs the full pipeline sequentially with logging. Pylance-compliant.
"""

from __future__ import annotations

import os
import logging
import sys
from dotenv import load_dotenv
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# Core pipeline imports (all at top)
from core.store_news import (
    ArticleRepository,
    filter_articles_by_keywords,
)
from core.utils import ConfigLoader, NotificationFormatter
from core.fetch_rss_news import fetch_rss_articles
from core.send_telegram import send_telegram_messages_in_chunks
from core.daily_digest_agent import DailyDigestAgent

# Configure logging FIRST to capture all messages
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("news_pipeline_debug.log", encoding="utf-8"),
    ],
)
logger: logging.Logger = logging.getLogger(__name__)

load_dotenv()
# --- Constants and Configuration ---
PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR: str = os.path.join(PROJECT_ROOT, "news_feed_pipeline/data")
CONFIG_DIR: str = os.path.join(PROJECT_ROOT, "news_feed_pipeline/config")
NEWS_SOURCES_CONFIG_FILE: str = os.path.join(CONFIG_DIR, "news_sources.json")
KEYWORDS_CONFIG_FILE: str = os.path.join(CONFIG_DIR, "keywords.json")
DB_NAME: str = "news_feed.db"

# Telegram credentials from environment
BOT_TOKEN: Optional[str] = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID: Optional[str] = os.getenv("TELEGRAM_CHAT_ID")
API_KEY: Optional[str] = os.getenv("OPENAI_API_KEY")
MODEL: Optional[str] = os.getenv("MODEL_NAME")
print(API_KEY)

# Ensure directories exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(CONFIG_DIR, exist_ok=True)


# --- Type Aliases ---
ArticleDict = Dict[str, Any]
SourceConfig = Dict[str, str]
ArticlesList = List[List[ArticleDict]]


# --- Core Pipeline Functions ---
def initialize_db(db_path: str) -> str:
    """Initialize SQLite database for news articles."""
    logger.info("Initializing database: %s", db_path)
    repo: ArticleRepository = ArticleRepository(db_path)
    repo.initialize_db()
    logger.info("Database initialized successfully.")
    return db_path


def fetch_all_headlines(sources_config: List[SourceConfig]) -> ArticlesList:
    """Fetch headlines from all configured RSS sources."""
    all_articles: ArticlesList = []

    for source_config in sources_config:
        source_name: Optional[str] = source_config.get("name")
        source_url: Optional[str] = source_config.get("url")
        logger.info("Fetching headlines from: %s (%s)", source_name, source_url)

        if not source_name or not source_url:
            logger.warning(
                "Skipping source with missing name or URL: %s", source_config
            )
            all_articles.append([])
            continue

        try:
            articles: List[ArticleDict] = fetch_rss_articles(source_url, source_name)

            if articles:
                for article in articles:
                    article["fetch_timestamp"] = datetime.now().isoformat()
                logger.info("Fetched %d articles from %s.", len(articles), source_name)
                all_articles.append(articles)
            else:
                logger.warning("No articles found for %s.", source_name)
                all_articles.append([])

        except (ValueError, ConnectionError, TimeoutError) as e:
            logger.error("Error fetching from %s: %s", source_name, str(e))
            all_articles.append([])

    return all_articles


def filter_and_store_news(
    all_articles_list: ArticlesList, db_path: str, keywords: List[str]
) -> List[ArticleDict]:
    """Filter articles by keywords and store new ones in database."""
    logger.info("Processing articles from %d sources", len(all_articles_list))

    # Flatten articles with type checking
    flattened_articles: List[ArticleDict] = [
        item
        for sublist in all_articles_list
        for item in sublist
        if isinstance(item, dict) and item
    ]

    if not flattened_articles:
        logger.info("No articles to process.")
        return []

    logger.info("Total fetched articles: %d", len(flattened_articles))

    # Filter by keywords
    articles_to_store: List[ArticleDict] = filter_articles_by_keywords(
        flattened_articles, keywords
    )
    logger.info(
        "Found %d relevant articles after keyword filtering.", len(articles_to_store)
    )

    # Store new articles
    repo: ArticleRepository = ArticleRepository(db_path)
    newly_added_articles: List[ArticleDict] = repo.add_articles(articles_to_store)
    logger.info("Stored %d new articles in database.", len(newly_added_articles))

    return newly_added_articles


def generate_telegram_chunks(new_articles: List[ArticleDict]) -> List[str]:
    """Generate formatted Telegram message chunks from new articles."""
    formatter: NotificationFormatter = NotificationFormatter()
    chunks: List[str] = formatter.generate_telegram_message_chunks(new_articles)
    logger.info("Generated %d Telegram message chunks.", len(chunks))
    return chunks


def send_telegram_notifications(
    chunks: List[str], bot_token: Optional[str], chat_id: Optional[str]
) -> None:
    """Send Telegram notifications in chunks."""
    if not bot_token or not chat_id:
        logger.error("Telegram credentials missing. Skipping notifications.")
        logger.info("Message chunks ready (not sent): %d", len(chunks))
        return

    try:
        send_telegram_messages_in_chunks(bot_token, chat_id, chunks)
        logger.info("Successfully sent %d Telegram message chunks.")
    except (ValueError, ConnectionError, TimeoutError) as e:
        logger.error("Failed to send Telegram notifications: %s", str(e))


# --- Main Pipeline Runner ---
def run_pipeline() -> None:
    """Execute the complete news feed pipeline sequentially."""
    start_time: datetime = datetime.now()
    logger.info("=== Starting News Feed Pipeline (Standalone Debug Mode) ===")

    db_full_path: str = os.path.join(DATA_DIR, DB_NAME)
    new_articles_count: int = 0

    try:
        # Load configuration
        config_loader: ConfigLoader = ConfigLoader()

        sources: List[SourceConfig] = config_loader.load_news_sources(
            NEWS_SOURCES_CONFIG_FILE
        )
        keywords: List[str] = config_loader.load_keywords(KEYWORDS_CONFIG_FILE)

        logger.info(
            "Loaded %d news sources and %d keywords.", len(sources), len(keywords)
        )

        # Pipeline steps
        initialize_db(db_full_path)
        all_articles: ArticlesList = fetch_all_headlines(sources)
        new_articles: List[ArticleDict] = filter_and_store_news(
            all_articles, db_full_path, keywords
        )
        new_articles_count = len(new_articles)

        if new_articles:
            telegram_chunks: List[str] = generate_telegram_chunks(new_articles)
            daily_digest_agent: DailyDigestAgent = DailyDigestAgent(api_key=API_KEY, model_name=MODEL)
            result = daily_digest_agent.run_daily_digest_agent(new_articles, '2025-12-23')
            logger.info("Daily digest agent completed successfully. \n %s", result)
            #send_telegram_notifications(telegram_chunks, BOT_TOKEN, CHAT_ID)
        else:
            logger.info("No new relevant articles found. No notifications sent.")

        # Success summary
        total_time: timedelta = datetime.now() - start_time
        logger.info("=== Pipeline completed successfully in %s ===", total_time)
        logger.info("- Database: %s", db_full_path)
        logger.info("- New articles stored: %d", new_articles_count)

    except Exception as e:
        logger.error("Pipeline failed with error: %s", str(e))
        raise


if __name__ == "__main__":
    run_pipeline()
