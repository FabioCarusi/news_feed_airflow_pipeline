"""
Script autonomo per leggere e visualizzare gli articoli direttamente
dal database news_feed.db, senza modificare le classi esistenti.

Esecuzione:
    python /home/carusifabio/progetti/airflow/dags/read_db.py
"""

import sqlite3
import os
import pprint
import json

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(PROJECT_ROOT, "news_feed.db")

if not os.path.exists(DB_PATH):
    print(f"Errore: Il database '{DB_PATH}' non è stato trovato.")
    print("Assicurati di aver eseguito la pipeline Airflow almeno una volta.")


def read_latest_articles(db_conn, limit: int = 10):
    """
    Si connette al database SQLite, esegue una query per leggere gli ultimi
    articoli inseriti e li restituisce come lista di dizionari.

    Args:
        db_conn: Un oggetto cursor di sqlite3 già connesso al database.
        limit (int): Il numero massimo di articoli da recuperare.

    Returns:
        list[dict]: Una lista contenente gli articoli recuperati.
    """
    db_cursor = db_conn.cursor()
    db_cursor.execute(
        """
        SELECT url, title, source, match_keywords, ingestion_timestamp
        FROM articles
        ORDER BY ingestion_timestamp DESC
        LIMIT ?
        """,
        (limit,),
    )

    articles = [dict(row) for row in db_cursor.fetchall()]
    for article in articles:
        if article.get("match_keywords"):
            article["match_keywords"] = json.loads(article["match_keywords"])
        if article:
            pprint.pprint(article)
        else:
            print("Nessun articolo trovato.")

          
def read_logs(db_conn, limit: int):
    """
    Funzione per leggere i log
    """
    cur = db_conn.cursor()
    cur.execute(
        """
        SELECT * 
        FROM logs
        ORDER BY timestamp DESC
        LIMIT ?
        """,
        (limit, )
    )
    logs = [dict(row) for row in cur.fetchall()]
    for l in logs:
        pprint.pprint(l)

def delete_latest_articles(db_conn, limit: int):
    """
    Cancella gli ultimi 'limit' articoli inseriti nel database.

    Per sicurezza, prima elenca gli articoli che verranno rimossi e
    chiede conferma all'utente prima di eseguire la cancellazione.

    Args:
        db_conn 
        limit (int): Il numero di articoli da cancellare.
    """

    db_cursor = db_conn.cursor()
    db_cursor.execute(
        "SELECT id, title FROM articles ORDER BY id DESC LIMIT ?", (limit,)
    )
    articles_to_delete = db_cursor.fetchall()

    if not articles_to_delete:
        print("Nessun articolo da cancellare trovato.")
        return

    print("--- I seguenti articoli verranno cancellati ---")
    for article_id, title in articles_to_delete:
        print(f"  [ID: {article_id}] {title}")

    confirm = input(
        f"\nSei sicuro di voler cancellare questi {len(articles_to_delete)} articoli? (s/n): "
    ).lower()
    if confirm != "s":
        print("Operazione annullata dall'utente.")
        return

    ids_to_delete = [row[0] for row in articles_to_delete]
    placeholders = ",".join("?" for _ in ids_to_delete)
    db_conn.execute(f"DELETE FROM articles WHERE id IN ({placeholders})", ids_to_delete)

    db_conn.commit()

if __name__ == "__main__":
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = (
            sqlite3.Row
        )

    delete_latest_articles(conn, limit=10)
    read_logs(conn, limit=10)
    #read_latest_articles(conn, limit=10)
