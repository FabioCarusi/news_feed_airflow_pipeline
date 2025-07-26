# include/store_news.py
import sqlite3
import os
import logging
from datetime import datetime
import json # Per serializzare le liste di keywords/tag

# dag_logger principale del DAG
dag_logger = logging.getLogger(__name__)

def get_db_path(data_dir: str, db_name: str) -> str:
    """Ritorna il percorso completo al database."""
    return os.path.join(data_dir, db_name)

def init_db(data_dir: str, db_name: str = 'news_feed.db'):
    """Inizializza il database e crea le tabelle se non esistono."""
    
    # Assicura che la directory dei dati esista. sqlite3 non la crea automaticamente.
    os.makedirs(data_dir, exist_ok=True) 

    db_path = get_db_path(data_dir, db_name)
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Tabella per le notizie
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL, -- URL unico per prevenire duplicati
                title TEXT NOT NULL,
                source TEXT,
                fetch_timestamp TEXT NOT NULL,
                match_keywords TEXT, -- Keywords che hanno matchato l'articolo (JSON string)
                ingestion_timestamp TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Tabella per i metadati generali dell'esecuzione della pipeline
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pipeline_runs_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dag_run_id TEXT NOT NULL,
                execution_timestamp TEXT NOT NULL,
                pipeline_name TEXT NOT NULL,
                status TEXT NOT NULL,
                items_processed INTEGER,      -- Es. articoli scaricati
                new_items_stored INTEGER,     -- Es. articoli filtrati e aggiunti al DB
                error_message TEXT,
                log_timestamp TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        conn.commit()
        dag_logger.info(f"Database '{db_name}' initialized and tables created/verified at {db_path}")
    except sqlite3.Error as e:
        dag_logger.error(f"Error initializing database at {db_path}: {e}")
        raise # Rilancia l'eccezione per fermare il task di Airflow
    finally:
        if conn:
            conn.close()

def store_filtered_news(db_path: str, articles: list[dict], keywords: list[str]) -> tuple[int, list[dict]]:
    """
    Filtra le notizie per keyword, le salva nel DB (evitando duplicati)
    e ritorna il conteggio delle nuove notizie e la lista di quelle appena aggiunte.
    """
    dag_logger.info(f"Starting filter and store process for {len(articles)} total articles fetched.")
    conn = None
    new_articles_count = 0
    newly_added_articles = [] # Lista per gli articoli effettivamente inseriti
    
    # Prepara le keywords per il matching case-insensitive e per trovare parole complete
    lower_keywords = [k.lower() for k in keywords]

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        for article in articles:
            title = article.get('title', '').strip()
            url = article.get('url', '').strip()
            source = article.get('source', 'N/A')
            fetch_timestamp = article.get('fetch_timestamp', datetime.now().isoformat())

            if not url or not title:
                dag_logger.warning(f"Skipping article due to missing URL or Title: {article}")
                continue

            # Filtra per keyword: cerca match nel titolo
            matched_keywords = []
            title_lower = title.lower()
            
            for keyword in lower_keywords:
                # Controlla se la keyword (o una parte di essa se è una frase) è nel titolo
                if keyword in title_lower:
                    matched_keywords.append(keyword)
            
            # Se nessuna keyword matcha, skippiamo l'articolo
            if not matched_keywords:
                dag_logger.debug(f"Article '{title}' does not match any keywords. Skipping.")
                continue

            # Inserimento nel DB con gestione dei duplicati (UNIQUE su URL)
            try:
                cursor.execute('''
                    INSERT INTO articles (url, title, source, fetch_timestamp, match_keywords)
                    VALUES (?, ?, ?, ?, ?)
                ''', (url, title, source, fetch_timestamp, json.dumps(matched_keywords))) # Salva keywords come JSON string
                
                conn.commit()
                new_articles_count += 1
                # Aggiungi l'articolo completo alla lista delle novità
                newly_added_articles.append(article) 
                dag_logger.info(f"New article stored: '{title}' from {source}")
            except sqlite3.IntegrityError:
                # Questo significa che l'URL esiste già (duplicato), non è un errore da loggare come critico
                dag_logger.debug(f"Article already exists (URL: {url}). Skipping insertion.")
            except Exception as e:
                # Altri errori durante l'inserimento
                dag_logger.error(f"Error inserting article '{title}' ({url}): {e}")
                conn.rollback() # Esegue un rollback per l'operazione corrente fallita

    except sqlite3.Error as e:
        dag_logger.error(f"Database error during filter and store operation: {e}")
        # Non rilanciare qui per non far fallire l'intero DAG se c'è un problema generico dopo aver processato alcuni articoli
    finally:
        if conn:
            conn.close()
            
    return new_articles_count, newly_added_articles
