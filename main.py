from airflow.decorators import dag, task
from airflow.sdk import Variable
import os
import logging
from datetime import datetime, timedelta
import json

# Importa le funzioni dai tuoi file nella cartella 'include'
from webscraping_airflow_pipeline.include.fetch_news import fetch_headlines_from_source
from webscraping_airflow_pipeline.include.store_news import (
    init_db,
    store_filtered_news,
    get_db_path,
)
from webscraping_airflow_pipeline.include.utils import (
    load_news_sources_config,
    load_keywords_config,
    generate_telegram_message_chunks)
from webscraping_airflow_pipeline.include.send_telegram import send_telegram_messages_in_chunks

# --- Configurazione Generale del Progetto ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_DIR = os.path.join(PROJECT_ROOT, "webscraping_airflow_pipeline/data")
CONFIG_DIR = os.path.join(PROJECT_ROOT, "webscraping_airflow_pipeline/config")
NEWS_SOURCES_CONFIG_FILE = os.path.join(
    CONFIG_DIR, "news_sources.json"
)  # Percorso al file di config
KEYWORDS_CONFIG_FILE = os.path.join(CONFIG_DIR, "keywords.json")

# Logger principale del DAG
dag_logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="news_feed_pipeline",
    default_args=default_args,
    description="A personalized news feed pipeline with multi-site scraping and keyword filtering.",
    start_date=datetime.now(),
    tags=["news", "data_pipeline", "scraping", "email"],
    catchup=False,
    schedule="0 8 * * *",
)
def news_feed_pipeline():

    @task
    def initialize_db_task():
        """Inizializza il database per il feed di notizie."""
        db_name = "news_feed.db"
        init_db(DATA_DIR, db_name)
        return get_db_path(DATA_DIR, db_name)

    @task
    def fetch_all_headlines(source_config: dict):
        """
        Task mappato per il fetching delle notizie da una singola fonte.
        """
        source_name = source_config["name"]
        source_url = source_config["url"]
        title_selector = source_config["title_selector"]
        link_selector = source_config["link_selector"]

        dag_logger.info(f"Fetching headlines from: {source_name} ({source_url})")

        articles = fetch_headlines_from_source(
            source_url, title_selector, link_selector
        )

        if not articles:
            dag_logger.warning(f"No articles found for {source_name}.")
            return []

        for article in articles:
            article["source"] = source_name
            article["fetch_timestamp"] = datetime.now().isoformat()

        dag_logger.info(f"Fetched {len(articles)} articles from {source_name}.")
        return articles

    @task
    def filter_and_store_all_news(
        all_fetched_articles_list: list, db_path: str, keywords: list
    ):
        """
        Filtra le notizie per keyword e le memorizza nel DB.
        """

        flattened_articles = [
            item for sublist in all_fetched_articles_list for item in sublist if item
        ]

        if not flattened_articles:
            dag_logger.info("No articles to process after fetching from all sources.")
            return []

        dag_logger.info(
            f"Total fetched articles across all sources: {len(flattened_articles)}"
        )

        new_articles_count, newly_added_articles = store_filtered_news(
            db_path, flattened_articles, keywords
        )

        dag_logger.info(
            f"Successfully filtered and stored {new_articles_count} new articles."
        )

        return newly_added_articles

    @task
    def generate_telegram_chunks_task(newly_added_articles: list): # Renamed to reflect its purpose
        """Generates a list of HTML message chunks for Telegram."""
        dag_logger.info(
            f"Generating Telegram chunks for {len(newly_added_articles)} new articles."
        )
        # Call the new chunking function
        telegram_message_chunks = generate_telegram_message_chunks(newly_added_articles, chunk_size=10)
        dag_logger.info(f"Generated {len(telegram_message_chunks)} chunks for Telegram.")
        return telegram_message_chunks

    @task
    def send_telegram_notification_task(telegram_message_chunks: list[str]): # Now accepts a list
        """Sends the notification to Telegram in chunks."""
        bot_token = Variable.get("TELEGRAM_BOT_TOKEN")
        chat_id = Variable.get("TELEGRAM_CHAT_ID")

        if not all([bot_token, chat_id]):
            dag_logger.error(
                f"Telegram credentials BOT_TOKEN ({bot_token}) or CHAT_ID ({chat_id}) missing. Cannot send notification."
            )
            raise ValueError("Telegram credentials not configured correctly.")

        # Call the function that sends messages in chunks
        send_telegram_messages_in_chunks(bot_token=bot_token, chat_id=chat_id, messages=telegram_message_chunks)

    # --- Definizione delle Dipendenze ---
    _sources_to_fetch = load_news_sources_config(NEWS_SOURCES_CONFIG_FILE)
    _keywords_to_use = load_keywords_config(KEYWORDS_CONFIG_FILE)

    # 1. Inizializzazione DB (indipendente dagli altri caricamenti)
    _db_path = initialize_db_task()

    # 2. Fetch delle notizie. Dipende dalle fonti caricate.
    _all_fetched_articles = fetch_all_headlines.partial().expand(
        source_config=_sources_to_fetch
    )
    
    _db_path >> _all_fetched_articles

    # 3. Filter and store news. Depends on fetch completion, DB init, and keyword loading.
    _filtered_and_stored_news = filter_and_store_all_news(
        _all_fetched_articles, _db_path, _keywords_to_use
    )

    [_db_path, _all_fetched_articles] >> _filtered_and_stored_news

    # 4. Generate Telegram chunks
    _telegram_message_chunks = generate_telegram_chunks_task(_filtered_and_stored_news)
    
    # 5. Send Telegram chunks
    # This task depends on the chunk generation.
    _telegram_message_chunks >> send_telegram_notification_task(_telegram_message_chunks)


# Instantiate the DAG
news_feed_pipeline()