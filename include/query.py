import sqlite3
import os
from pprint import pprint # Per una stampa più leggibile dei risultati

# --- Configurazione del percorso del Database ---
# Assicurati che PROJECT_ROOT punti alla cartella che contiene 'dags' e 'include'
# Se esegui questo script dalla stessa cartella del DAG, puoi adattare il percorso.
# Questo esempio assume che 'query_db.py' sia nella radice del tuo progetto.
PROJECT_ROOT = os.path.dirname(os.path.abspath(__name__))
DATA_DIR = os.path.join(PROJECT_ROOT, 'db')
DB_PATH = os.path.join(DATA_DIR, 'news_pipeline_db.db')

def execute_query(query: str, fetch_all=True):
    """Esegue una query SQL e stampa i risultati."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(query)
        
        if fetch_all:
            # Recupera i nomi delle colonne
            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
            
            print(f"\n--- Risultati per la query: ---\n{query}\n------------------------------")
            if rows:
                # Stampa le intestazioni
                print(columns)
                # Stampa le righe
                for row in rows:
                    print(row)
            else:
                print("Nessun risultato trovato.")
            
            return columns, rows
        else:
            conn.commit()
            print(f"\n--- Query eseguita con successo (nessun fetch richiesto): ---\n{query}\n------------------------------")
            return None, None

    except sqlite3.Error as e:
        print(f"\n!!! ERRORE SQLite: {e} !!!")
        print(f"Query fallita: {query}")
    finally:
        if conn:
            conn.close()

# --- Esempi di Query ---

if __name__ == "__main__":
    print(f"Tentativo di connessione al database: {DB_PATH}")
    if not os.path.exists(DB_PATH):
        print(f"ERRORE: Database non trovato al percorso specificato: {DB_PATH}")
        print("Assicurati che la pipeline Airflow abbia già creato il DB e che il percorso sia corretto.")
    else:
        # Query 1: Seleziona tutti i log di esecuzione della pipeline
        query_1 = "SELECT * FROM pipeline_runs_metadata ORDER BY execution_timestamp DESC;"
        execute_query(query_1)

        # Query 2: Seleziona solo le esecuzioni fallite
        query_2 = "SELECT dag_run_id, source_url, error_message FROM pipeline_runs_metadata WHERE status = 'failed';"
        execute_query(query_2)

        # Query 3: Conta gli articoli processati per ogni esecuzione di successo
        query_3 = "SELECT dag_run_id, articles_processed, source_url FROM pipeline_runs_metadata WHERE status = 'success' ORDER BY execution_timestamp DESC LIMIT 5;"
        execute_query(query_3)

        # Query 4: Seleziona tutti gli articoli di notizie processati
        query_4 = "SELECT id, url, title, sentiment, category FROM processed_news ORDER BY process_timestamp DESC LIMIT 10;"
        execute_query(query_4)

        # Query 5: Conta gli articoli per sorgente (dominio)
        query_5 = "SELECT source, COUNT(*) AS num_articles FROM processed_news GROUP BY source ORDER BY num_articles DESC;"
        execute_query(query_5)

        # Query 6: Cerca articoli con un titolo contenente una parola chiave
        keyword = "Airflow"
        query_6 = f"SELECT url, title FROM processed_news WHERE title LIKE '%{keyword}%' LIMIT 5;"
        execute_query(query_6)

        print("\n--- Fine delle query ---")