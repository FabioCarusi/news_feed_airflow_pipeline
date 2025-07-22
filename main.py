from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

import os
import logging
from datetime import datetime, timedelta

# Importa le funzioni dai tuoi file nella cartella 'include'
from newspaper_pipeline.include.store_data import init_db, log_pipeline_metadata, store_processed_news, get_db_path
from newspaper_pipeline.include.fetch_data import fetch_news_article

# Assicurati che la cartella 'include' sia riconosciuta come un pacchetto Python
# creando un file __init__.py vuoto all'interno di essa.

# --- Configurazione Generale ---
# DATA_DIR deve essere accessibile sia dal DAG che dalle funzioni incluse.
# Lo definiamo qui e lo passiamo dove serve.
# La radice del progetto dovrebbe essere la cartella che contiene 'dags' e 'include'
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__name__)))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
os.makedirs(DATA_DIR, exist_ok=True) # Crea la cartella dati se non esiste

# Inizializza il logger principale del DAG
dag_logger = logging.getLogger(__name__)

# Lista di URL da processare
NEWS_URLS = [
    "https://www.reddit.com/r/dataengineering/",
    "https://www.reddit.com/r/apache_airflow/",
    "https://www.reddit.com/r/Python/"
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='news_pipeline_multi_url', # Nuovo nome per riflettere il multi-URL
    default_args=default_args,
    description='A custom news pipeline fetching, processing, and storing articles from multiple URLs.',
    start_date=datetime.now(),
    tags=['news', 'data_pipeline', 'scraping', 'multi_url'],
    catchup=False,
)
def news_data_pipeline_multi_url():

    # Task iniziale per inizializzare il DB una sola volta per DAG run
    @task
    def initialize_database():
        db_path = get_db_path(DATA_DIR)
        init_db(db_path)
        return db_path # Passa il percorso del DB via XCom

    db_path_xcom = initialize_database()

    # --- Task per il Fetch e la Logica per ogni URL ---
    @task
    def process_single_url(url: str, db_path: str, dag_run_id: str, execution_timestamp: str):
        logger = logging.getLogger(__name__)

        # Step 1: Fetch dell'articolo
        fetch_result = fetch_news_article(url, DATA_DIR)

        # Log dei metadati del singolo URL (successo/fallimento)
        log_pipeline_metadata(
            dag_run_id=dag_run_id,
            execution_timestamp=execution_timestamp,
            source_url=url,
            status=fetch_result['status'],
            error_message=fetch_result.get('error_message')
        )

        if fetch_result['status'] == 'failed':
            logger.warning(f"Skipping further processing for {url} due to fetch failure.")
            return None # Non passare dati al task successivo se il fetch è fallito

        # Step 2: (Simulazione) Pulizia e Estrazione (Placeholder)
        # Qui andrebbe la tua logica di pulizia, per ora simuliamo solo
        processed_data = {
            'url': fetch_result['article_url'],
            'title': fetch_result['article_title'],
            'raw_text': fetch_result['raw_text_file_path'], # O il contenuto effettivo
            'processed_text': f"Processed text from {fetch_result['article_url']}",
            'sentiment': 'neutral', # Placeholder
            'entities': '[]', # Placeholder
            'category': 'general', # Placeholder
            'fetch_timestamp': fetch_result['fetch_timestamp']
        }
        logger.info(f"Simulated cleaning and processing for {url}")

        # Step 3: (Simulazione) Analisi e Arricchimento (Placeholder)
        # Qui andrebbe la tua logica di analisi, per ora simuliamo solo
        processed_data['sentiment'] = 'positive' if 'data' in processed_data['processed_text'].lower() else 'negative'
        processed_data['category'] = 'Tech' if 'engineering' in processed_data['processed_text'].lower() else 'Other'
        logger.info(f"Simulated analysis and enrichment for {url}")


        # Step 4: Memorizzazione nel DB
        store_processed_news(processed_data)
        logger.info(f"Article from {url} stored in DB.")

        return processed_data # Ritorna i dati processati per eventuali utilizzi futuri


    # Mappa il task 'process_single_url' su ogni URL nella lista NEWS_URLS
    # Ogni URL sarà un'istanza del task 'process_single_url'
    processed_articles = process_single_url.partial(
        db_path=db_path_xcom,
        dag_run_id="{{ dag_run.run_id }}",
        execution_timestamp="{{ ds_nodash }}_{{ ts_nodash | replace(':', '') | replace('-', '') }}"
    ).expand(url=NEWS_URLS)


# Devi chiamare la funzione del DAG per registrarlo in Airflow
news_data_pipeline_multi_url()