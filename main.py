"""
News Feed Pipeline DAG.

This DAG orchestrates a personalized news feed pipeline that:
1. Initializes a SQLite database to store news articles
2. Fetches news headlines from various RSS sources defined in `news_sources.json`
3. Filters articles based on keywords from `keywords.json`
4. Stores relevant articles in the database, avoiding duplicates
5. Sends notifications via Telegram
6. Provides email notifications for DAG success and task failures

The DAG is scheduled to run daily at 8:00 AM UTC.
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Any

from airflow.decorators import dag, task
from airflow.sdk import Variable
from airflow.sdk import get_current_context



from news_feed_pipeline.core.fetch_rss_news import fetch_rss_articles  # type: ignore
from news_feed_pipeline.core.send_telegram import (  # type: ignore
    send_telegram_messages_in_chunks,
)
from news_feed_pipeline.core.store_news import (  # type: ignore
    ArticleRepository,
    filter_articles_by_keywords,
)
from news_feed_pipeline.core.utils import (  # type: ignore
    AirflowCallbackHandler,
    ConfigLoader,
    NotificationFormatter,
)

from news_feed_pipeline.core.daily_digest_agent import DailyDigestAgent  # type: ignore

# --- Configuration ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "news_feed_pipeline/data")
CONFIG_DIR = os.path.join(PROJECT_ROOT, "news_feed_pipeline/config")
NEWS_SOURCES_CONFIG_FILE = os.path.join(CONFIG_DIR, "news_sources.json")
KEYWORDS_CONFIG_FILE = os.path.join(CONFIG_DIR, "keywords.json")
BOT_TOKEN = Variable.get(key="TELEGRAM_BOT_TOKEN", default=None)
CHAT_ID = Variable.get(key="TELEGRAM_CHAT_ID", default=None)
API_KEY = Variable.get(key="OPENAI_API_KEY", default=None)
MODEL_NAME = Variable.get(key="MODEL_NAME", default=None)
FALLBACK_MODEL_NAME = Variable.get(key="FALLBACK_MODEL_NAME", default="meta-llama/llama-3.3-70b-instruct:free")

logger = logging.getLogger(__name__)

# Initialize utilities
config_loader = ConfigLoader()
formatter = NotificationFormatter()
callbacks = AirflowCallbackHandler(formatter=formatter)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": callbacks.send_task_failure_email,
}


@dag(
    dag_id="news_feed_pipeline",
    default_args=default_args,
    description="Personalized news feed pipeline with RSS scraping and keyword filtering.",
    start_date=datetime.now(),
    tags=["news", "data_pipeline", "scraping", "email"],
    catchup=False,
    schedule="0 8 * * *",
    on_success_callback=callbacks.send_dag_success_email,
)
def news_feed_pipeline() -> None:
    """Define the news feed pipeline DAG."""

    @task
    def initialize_db_task() -> str:
        """Initialize the database for the news feed.

        Returns:
            str: Path to the initialized database.
        """
        db_name = "news_feed.db"
        db_path = os.path.join(DATA_DIR, db_name)

        repo = ArticleRepository(db_path)
        repo.initialize_db()
        logger.info("Database initialized successfully at: %s", db_path)

        return db_path

    @task
    def fetch_all_headlines(source_config: dict[str, Any]) -> list[dict[str, Any]]:
        """Fetch news from a single RSS source.

        Args:
            source_config: Configuration dictionary containing 'name' and 'url'.

        Returns:
            List of fetched articles with metadata.
        """
        source_name = source_config.get("name")
        source_url = source_config.get("url")

        if not source_name or not source_url:
            logger.warning(
                "Skipping source with missing name or URL: %s", source_config
            )
            return []

        logger.info("Fetching headlines from: %s (%s)", source_name, source_url)
        articles = fetch_rss_articles(source_url, source_name)

        if not articles:
            logger.warning("No articles found for %s", source_name)
            return []

        # Add fetch timestamp to each article
        fetch_time = datetime.now().isoformat()
        for article in articles:
            article["fetch_timestamp"] = fetch_time

        logger.info("Fetched %d articles from %s", len(articles), source_name)
        return articles

    @task
    def filter_all_news(
        all_fetched_articles_list: list[list[dict[str, Any]]],
        keywords: list[str],
    ) -> list[dict[str, Any]]:
        """Filter articles by keywords WITHOUT storing to database.

        Args:
            all_fetched_articles_list: Nested list of articles from all sources.
            keywords: List of keywords to filter articles.

        Returns:
            List of filtered articles (not yet stored).
        """
        # Flatten the nested list of articles
        flattened_articles = [
            article
            for sublist in all_fetched_articles_list
            for article in sublist
            if article
        ]

        if not flattened_articles:
            logger.info("No articles to process after fetching from all sources")
            return []

        logger.info(
            "Total fetched articles across all sources: %d", len(flattened_articles)
        )

        # Filter articles by keywords
        filtered_articles = filter_articles_by_keywords(
            articles=flattened_articles, keywords=keywords
        )

        logger.info("Filtered %d articles matching keywords", len(filtered_articles))
        return filtered_articles

    @task
    def store_sent_articles(
        filtered_articles: list[dict[str, Any]],
        db_path: str,
    ) -> int:
        """Store articles in database AFTER successful Telegram send.

        Args:
            filtered_articles: List of filtered articles to store.
            db_path: Path to the SQLite database.

        Returns:
            Number of newly stored articles.
        """
        if not filtered_articles:
            logger.info("No articles to store")
            return 0

        repo = ArticleRepository(db_path)
        newly_added_articles = repo.add_articles(filtered_articles)

        logger.info("Successfully stored %d new articles", len(newly_added_articles))
        return len(newly_added_articles)

    @task
    def generate_telegram_chunks_task(
        newly_added_articles: list[dict[str, Any]],
    ) -> list[str]:
        """Generate HTML message chunks for Telegram.

        Args:
            newly_added_articles: List of newly added articles.

        Returns:
            List of formatted message chunks ready for Telegram.
        """
        logger.info(
            "Generating Telegram chunks for %d new articles", len(newly_added_articles)
        )

        telegram_message_chunks = formatter.generate_telegram_message_chunks(
            newly_added_articles
        )

        logger.info("Generated %d chunks for Telegram", len(telegram_message_chunks))
        return telegram_message_chunks

    @task
    def send_telegram_notification_task(
        telegram_message_chunks: list[str],
        bot_token: str | None,
        chat_id: str | None,
    ) -> int:
        """Send Telegram notifications in chunks.

        Args:
            telegram_message_chunks: List of message chunks to send.
            bot_token: Telegram bot token.
            chat_id: Telegram chat ID.

        Returns:
            Number of chunks sent.

        Raises:
            ValueError: If Telegram credentials are not configured.
        """
        if not bot_token or not chat_id:
            logger.error("Telegram credentials missing. Cannot send notification")
            raise ValueError("Telegram credentials not configured correctly")

        send_telegram_messages_in_chunks(
            bot_token=bot_token,
            chat_id=chat_id,
            messages=telegram_message_chunks,
        )

        logger.info(
            "Successfully sent %d Telegram message chunks", len(telegram_message_chunks)
        )
        return len(telegram_message_chunks)

    @task
    def run_daily_digest_agent_task(articles: list[dict[str, Any]]) -> str:
        context = get_current_context()
        ds = context.get('ds', datetime.now().strftime('%Y-%m-%d'))
        agent = DailyDigestAgent(API_KEY, MODEL_NAME, fallback_model_name=FALLBACK_MODEL_NAME)
        return agent.run_daily_digest_agent(articles, date_str=ds)

    # DAG workflow definition
    sources_to_fetch = config_loader.load_news_sources(NEWS_SOURCES_CONFIG_FILE)
    keywords_to_use = config_loader.load_keywords(KEYWORDS_CONFIG_FILE)

    db_path = initialize_db_task()

    all_fetched_articles = fetch_all_headlines.partial().expand(
        source_config=sources_to_fetch
    )

    # Filter articles WITHOUT storing (idempotency fix)
    filtered_news = filter_all_news(
        all_fetched_articles, keywords_to_use
    )

    # Generate daily digest message
    agent_message = run_daily_digest_agent_task(filtered_news)
    
    # Send to Telegram
    send_telegram_notification_task([agent_message], BOT_TOKEN, CHAT_ID)
    
    # Store articles ONLY AFTER successful send (idempotency fix)
    store_sent_articles(filtered_news, db_path)



# Instantiate the DAG
news_feed_pipeline()
