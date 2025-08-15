from airflow.decorators import dag, task
from airflow.sdk import Variable
import os
import logging
from datetime import datetime, timedelta

# Import functions from the 'include' folder
from webscraping_airflow_pipeline.include.store_news import (
    ArticleRepository,
    filter_articles_by_keywords,
)
from webscraping_airflow_pipeline.include.utils import (
    ConfigLoader,
    NotificationFormatter,
    AirflowCallbackHandler,
)
from webscraping_airflow_pipeline.include.send_telegram import (
    send_telegram_messages_in_chunks,
)
from webscraping_airflow_pipeline.include.fetch_rss_news import fetch_rss_articles
from webscraping_airflow_pipeline.include.log_handler import task_db_logger


# --- General Project Configuration ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_DIR = os.path.join(PROJECT_ROOT, "webscraping_airflow_pipeline/data")
CONFIG_DIR = os.path.join(PROJECT_ROOT, "webscraping_airflow_pipeline/config")
NEWS_SOURCES_CONFIG_FILE = os.path.join(CONFIG_DIR, "news_sources.json")
KEYWORDS_CONFIG_FILE = os.path.join(CONFIG_DIR, "keywords.json")
BOT_TOKEN = Variable.get("TELEGRAM_BOT_TOKEN")
CHAT_ID = Variable.get("TELEGRAM_CHAT_ID")


# Main logger
logger = logging.getLogger(__name__)


# Instantiate helper classes
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

    @task
    def initialize_db_task(ti=None):
        """Initializes the database for the news feed."""
        db_name = "news_feed.db"
        db_path = os.path.join(DATA_DIR, db_name)
        with task_db_logger(db_path, ti) as db_task_logger:
            repo = ArticleRepository(db_path)
            repo.initialize_db()
            db_task_logger.info("Database initialized successfully.")
        return db_path

    @task
    def fetch_all_headlines(source_config: dict, ti=None):
        """
        Mapped task for fetching news from a single source.
        """
        with task_db_logger(ti.xcom_pull(task_ids="initialize_db_task"), ti):
            source_name = source_config["name"]
            source_url = source_config["url"]

            logger.info(f"Fetching headlines from: {source_name} ({source_url})")

            articles = fetch_rss_articles(source_url, source_name)

            if not articles:
                logger.warning(f"No articles found for {source_name}.")
                return []

            for article in articles:
                article["fetch_timestamp"] = datetime.now().isoformat()

            logger.info(f"Fetched {len(articles)} articles from {source_name}.")
            return articles

    @task
    def filter_and_store_all_news(
        all_fetched_articles_list: list, db_path: str, keywords: list, ti=None
    ):
        """
        Filters news by keyword and stores them in the DB.
        """

        with task_db_logger(db_path, ti):
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
                f"Total fetched articles across all sources: {len(flattened_articles)}"
            )

            # 1. Business Logic: Filter articles based on keywords
            articles_to_store = filter_articles_by_keywords(
                articles=flattened_articles, keywords=keywords
            )

            # 2. Data Access: Store the filtered articles in the database
            repo = ArticleRepository(db_path)
            newly_added_articles = repo.add_articles(articles_to_store)

            logger.info(
                f"Successfully stored {len(newly_added_articles)} new articles."
            )

            return newly_added_articles

    @task
    def generate_telegram_chunks_task(
        newly_added_articles: list, db_path: str, ti=None
    ):  # Renamed to reflect its purpose
        """Generates a list of HTML message chunks for Telegram."""
        with task_db_logger(db_path, ti):
            logger.info(
                f"Generating Telegram chunks for {len(newly_added_articles)} new articles."
            )
            # Call the new chunking function
            telegram_message_chunks = formatter.generate_telegram_message_chunks(
                newly_added_articles
            )
            logger.info(
                f"Generated {len(telegram_message_chunks)} chunks for Telegram."
            )
            return telegram_message_chunks

    @task
    def send_telegram_notification_task(
        telegram_message_chunks: list[str],
        bot_token: str,
        chat_id: str,
        db_path: str,
        ti=None,
    ):
        """Sends notifications to Telegram in chunks."""
        with task_db_logger(db_path, ti):
            if not all([bot_token, chat_id]):
                logger.error(
                    f"Telegram credentials BOT_TOKEN ({bot_token}) or CHAT_ID ({chat_id}) missing. Cannot send notification."
                )
                raise ValueError("Telegram credentials not configured correctly.")

            # Call the function to send messages in chunks
            send_telegram_messages_in_chunks(
                bot_token=bot_token, chat_id=chat_id, messages=telegram_message_chunks
            )

    # --- Defining Dependencies ---
    _sources_to_fetch = config_loader.load_news_sources(NEWS_SOURCES_CONFIG_FILE)
    _keywords_to_use = config_loader.load_keywords(KEYWORDS_CONFIG_FILE)

    # 1. DB Initialization (independent of other loads)
    _db_path = initialize_db_task()

    # 2. Fetch news. Depends on the loaded sources.
    _all_fetched_articles = fetch_all_headlines.partial().expand(
        source_config=_sources_to_fetch
    )

    _db_path >> _all_fetched_articles

    # 3. Filter and store news. Depends on fetch completion, DB init, and keyword loading.
    _filtered_and_stored_news = filter_and_store_all_news(
        _all_fetched_articles, _db_path, _keywords_to_use
    )

    [_all_fetched_articles] >> _filtered_and_stored_news

    # 4. Generate Telegram chunks
    _telegram_message_chunks = generate_telegram_chunks_task(
        _filtered_and_stored_news, _db_path
    )

    # 5. Send Telegram chunks
    # This task depends on the chunk generation.
    _telegram_message_chunks >> send_telegram_notification_task(
        _telegram_message_chunks, BOT_TOKEN, CHAT_ID, _db_path
    )


# Instantiate the DAG
news_feed_pipeline()
