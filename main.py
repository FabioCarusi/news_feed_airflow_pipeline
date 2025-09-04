"""
This DAG orchestrates a personalized news feed pipeline.

It performs the following steps:
1. Initializes a SQLite database to store news articles.
2. Fetches news headlines from various RSS sources defined in `news_sources.json`.
3. Filters the fetched articles based on a predefined list of keywords from `keywords.json`.
4. Stores the newly found and relevant articles into the SQLite database, avoiding duplicates.
5. Generates and sends notifications of these new articles via Telegram.
6. Provides email notifications for DAG success (with article summary) and task failures.

The DAG is scheduled to run daily at 8:00 AM UTC.
"""

import os
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.sdk import Variable

from webscraping_airflow_pipeline.include.store_news import ( # type: ignore
    ArticleRepository,
    filter_articles_by_keywords,
)
from webscraping_airflow_pipeline.include.utils import ( # type: ignore
    ConfigLoader,
    NotificationFormatter,
    AirflowCallbackHandler,
)
from webscraping_airflow_pipeline.include.send_telegram import ( # type: ignore
    send_telegram_messages_in_chunks,
)
from webscraping_airflow_pipeline.include.fetch_rss_news import fetch_rss_articles #type: ignore
from webscraping_airflow_pipeline.include.log_handler import task_db_logger # type: ignore

# --- General Project Configuration ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "webscraping_airflow_pipeline/data")
CONFIG_DIR = os.path.join(PROJECT_ROOT, "webscraping_airflow_pipeline/config")
NEWS_SOURCES_CONFIG_FILE = os.path.join(CONFIG_DIR, "news_sources.json")
KEYWORDS_CONFIG_FILE = os.path.join(CONFIG_DIR, "keywords.json")
BOT_TOKEN = Variable.get(key="TELEGRAM_BOT_TOKEN", default=None)
CHAT_ID = Variable.get(key="TELEGRAM_CHAT_ID", default=None)

logger = logging.getLogger(__name__)

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
    description="A personalized news feed pipeline with multi-site scraping and keyword filtering.",
    start_date=datetime.now(),
    tags=["news", "data_pipeline", "scraping", "email"],
    catchup=False,
    schedule="0 8 * * *",
    on_success_callback=callbacks.send_dag_success_email,
)
def news_feed_pipeline():
    """Main DAG for the news pipeline."""

    @task
    def initialize_db_task(ti=None) -> str:
        """Initializes the database for the news feed."""
        db_name = "news_feed.db"
        db_path = os.path.join(DATA_DIR, db_name)
        with task_db_logger(db_path, ti) as conn:
            repo = ArticleRepository(db_path)
            repo.initialize_db(conn=conn)
            logger.info("Database initialized successfully.")
        return db_path

    @task
    def fetch_all_headlines(source_config: dict) -> list:
        """Task to fetch news from a single source."""
        source_name = source_config.get("name")
        source_url = source_config.get("url")
        logger.info("Fetching headlines from: %s (%s)", source_name, source_url)
        if not source_name or not source_url:
            logger.warning(
                "Skipping source with missing name or URL: %s", source_config
            )
            return []
        else:
            articles = fetch_rss_articles(source_url, source_name)
            if not articles:
                logger.warning("No articles found for %s.", source_name)
                return []
            for article in articles:
                article["fetch_timestamp"] = datetime.now().isoformat()
            logger.info("Fetched %d articles from %s.", len(articles), source_name)
            return articles

    @task
    def filter_and_store_all_news(
        all_fetched_articles_list, db_path, keywords, ti=None
    ) -> list:
        """Filters news by keyword and saves them to the DB."""
        with task_db_logger(db_path, ti) as conn:
            flattened_articles = [
                item
                for sublist in all_fetched_articles_list
                for item in sublist
                if item
            ]
            if not flattened_articles:
                logger.info("No articles to process after fetching from all sources.")
                return []
            logger.info(
                "Total fetched articles across all sources: %d",
                len(flattened_articles),
            )
            articles_to_store = filter_articles_by_keywords(
                articles=flattened_articles, keywords=keywords
            )
            repo = ArticleRepository(db_path)
            newly_added_articles = repo.add_articles(articles_to_store, conn=conn)
            logger.info(
                "Successfully stored %d new articles.",
                len(newly_added_articles),
            )
            return newly_added_articles

    @task
    def generate_telegram_chunks_task(newly_added_articles, db_path, ti=None) -> list:
        """Generates a list of HTML chunks for Telegram."""
        with task_db_logger(db_path, ti):  # Use logger for log consistency
            logger.info(
                "Generating Telegram chunks for %d new articles.",
                len(newly_added_articles),
            )
            telegram_message_chunks = formatter.generate_telegram_message_chunks(
                newly_added_articles
            )
            logger.info(
                "Generated %d chunks for Telegram.",
                len(telegram_message_chunks),
            )
            return telegram_message_chunks

    @task
    def send_telegram_notification_task(
        telegram_message_chunks,
        bot_token,
        chat_id,
        db_path,
        ti=None,
    ) -> int:
        """Sends Telegram notifications in chunks."""
        with task_db_logger(db_path, ti):
            if not bot_token or not chat_id:
                logger.error("Telegram credentials missing. Cannot send notification.")
                raise ValueError("Telegram credentials not configured correctly.")
            send_telegram_messages_in_chunks(
                bot_token=bot_token,
                chat_id=chat_id,
                messages=telegram_message_chunks,
            )
        return len(telegram_message_chunks)

    sources_to_fetch = config_loader.load_news_sources(NEWS_SOURCES_CONFIG_FILE)
    keywords_to_use = config_loader.load_keywords(KEYWORDS_CONFIG_FILE)

    db_path = initialize_db_task()
    all_fetched_articles = fetch_all_headlines.partial().expand(
        source_config=sources_to_fetch
    )
    filtered_and_stored_news = filter_and_store_all_news(
        all_fetched_articles, db_path, keywords_to_use
    )
    # [all_fetched_articles] >> filtered_and_stored_news

    telegram_message_chunks = generate_telegram_chunks_task(
        filtered_and_stored_news, db_path
    )
    # telegram_message_chunks >>
    send_telegram_notification_task(
        telegram_message_chunks, BOT_TOKEN, CHAT_ID, db_path
    )


# Instantiate the DAG
news_feed_pipeline()