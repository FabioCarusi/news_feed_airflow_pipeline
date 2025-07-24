from airflow.decorators import dag, task
import os
import logging
from datetime import datetime, timedelta
import json 

# Importa le funzioni dai tuoi file nella cartella 'include'
from newspaper_pipeline.include.fetch_news import fetch_headlines_from_source
from newspaper_pipeline.include.store_news import init_db, store_filtered_news, get_db_path
from newspaper_pipeline.include.utils import load_news_sources_config, load_keywords_config, generate_news_email_content
from newspaper_pipeline.include.send_telegram import send_telegram_message

# --- Configurazione Generale del Progetto ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_DIR = os.path.join(PROJECT_ROOT, "newspaper_pipeline/data")
CONFIG_DIR = os.path.join(PROJECT_ROOT, "newspaper_pipeline/config")
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
    def generate_notification_content_task(newly_added_articles: list):
        dag_logger.info(f"Generazione contenuto notifica per {len(newly_added_articles)} nuovi articoli.")
        notification_body = generate_news_email_content(newly_added_articles) 
        return notification_body

    
    @task
    def send_telegram_news(notification_body: str):
        """Invia la notifica su Telegram."""
        # Recupera le credenziali Telegram dalle variabili d'ambiente di Airflow
        bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")

        if not all([bot_token, chat_id]):
            dag_logger.error("Credenziali Telegram BOT_TOKEN o CHAT_ID mancanti. Impossibile inviare la notifica.")
            raise ValueError("Credenziali Telegram non configurate correttamente.")

        # Telegram ha un limite di 4096 caratteri per messaggio HTML.
        # Se il tuo body è più lungo, dovrai dividerlo o inviarlo come file.
        # Per ora, inviamo solo i primi 4000 caratteri se è troppo lungo.
        if len(notification_body) > 4096:
            dag_logger.warning("Contenuto del messaggio Telegram troppo lungo. Sarà troncato.")
            notification_body = notification_body[:4000] + "..."

        send_telegram_message(
            bot_token=bot_token,
            chat_id=chat_id,
            message=notification_body
        )
    
    # --- Definizione delle Dipendenze ---
    _sources_to_fetch = load_news_sources_config(NEWS_SOURCES_CONFIG_FILE)
    _keywords_to_use = load_keywords_config(KEYWORDS_CONFIG_FILE)    
    
    # 1. Inizializzazione DB (indipendente dagli altri caricamenti)
    _db_path = initialize_db_task() 

    # 2. Fetch delle notizie. Dipende dalle fonti caricate.
    _all_fetched_articles = fetch_all_headlines.partial().expand(source_config=_sources_to_fetch)
    
    # 3. Filtra e memorizza. Dipende dal fetch (che ha finito di aggregare tutti i risultati)
    #    e dall'inizializzazione del DB e dal caricamento delle keyword.
    _filtered_and_stored_news = filter_and_store_all_news(
        _all_fetched_articles,
        _db_path,
        _keywords_to_use
    )
    
    _notification_body = generate_notification_content_task(_filtered_and_stored_news)
    _send_news_to_telegram = send_telegram_news(_notification_body)
    
    _db_path >> _all_fetched_articles >> _filtered_and_stored_news
    _filtered_and_stored_news >> _notification_body >> _send_news_to_telegram


# Istanzia il DAG
news_feed_pipeline()

