# include/store_data.py
import sqlite3
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Nome del database
DB_NAME = 'news_pipeline_db.db'

def get_db_path(data_dir: str) -> str:
    """Ritorna il percorso completo al database."""
    return os.path.join(data_dir, DB_NAME)

def init_db(db_path: str):
    print(db_path)
    """Inizializza il database e crea le tabelle se non esistono."""
    conn = None
    try:
        conn = sqlite3.connect(f'{db_path}/{DB_NAME}')
        cursor = conn.cursor()

        # Tabella per i log principali della pipeline
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pipeline_runs_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dag_run_id TEXT NOT NULL,
                execution_timestamp TEXT NOT NULL,
                source_url TEXT,
                status TEXT NOT NULL,
                articles_processed INTEGER,
                error_message TEXT,
                log_timestamp TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Tabella per i dati delle notizie processate (schema semplificato per ora)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_news (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL,
                title TEXT,
                raw_text TEXT,
                processed_text TEXT,
                sentiment TEXT,
                entities TEXT,
                category TEXT,
                fetch_timestamp TEXT,
                process_timestamp TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        logger.info(f"Database {DB_NAME} initialized and tables created/verified at {db_path}")
    except sqlite3.Error as e:
        logger.error(f"Error initializing database: {e}")
        raise
    finally:
        if conn:
            conn.close()

def log_pipeline_metadata(dag_run_id: str, execution_timestamp: str, source_url: str,
                          status: str, articles_processed: int = None, error_message: str = None):
    """Registra i metadati di alto livello dell'esecuzione della pipeline."""
    logger.info(f"Logging pipeline metadata for DAG Run ID: {dag_run_id}")
    db_path = get_db_path(os.path.dirname(os.path.dirname(__file__))) # Risale per trovare DATA_DIR
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO pipeline_runs_metadata (dag_run_id, execution_timestamp, source_url, status, articles_processed, error_message)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (dag_run_id, execution_timestamp, source_url, status, articles_processed, error_message))
        conn.commit()
        logger.info("Pipeline metadata logged successfully.")
    except sqlite3.Error as e:
        logger.error(f"Failed to log pipeline metadata: {e}")
    finally:
        conn.close()

def store_processed_news(processed_data: dict):
    """Memorizza i dati delle notizie processate nel database."""
    logger.info(f"Storing processed news for URL: {processed_data.get('url', 'N/A')}")
    db_path = get_db_path(os.path.dirname(os.path.dirname(__file__))) # Risale per trovare DATA_DIR
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO processed_news (url, title, raw_text, processed_text, sentiment, entities, category, fetch_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            processed_data.get('url'),
            processed_data.get('title'),
            processed_data.get('raw_text'),
            processed_data.get('processed_text'),
            processed_data.get('sentiment'),
            processed_data.get('entities'), # Sarà un JSON string o None
            processed_data.get('category'),
            processed_data.get('fetch_timestamp')
        ))
        conn.commit()
        logger.info(f"Article '{processed_data.get('title', 'N/A')}' stored successfully.")
    except sqlite3.Error as e:
        logger.error(f"Failed to store article '{processed_data.get('title', 'N/A')}': {e}")
    finally:
        conn.close()
        