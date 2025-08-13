import pytest
import logging
import sqlite3
from unittest.mock import MagicMock

# Importa le classi e le funzioni da testare
from webscraping_airflow_pipeline.include.store_news import (
    ArticleRepository,
    filter_articles_by_keywords,
)
from webscraping_airflow_pipeline.include.utils import NotificationFormatter
from webscraping_airflow_pipeline.include.log_handler import task_db_logger

# --- Fixtures: Dati e Risorse Riutilizzabili per i Test ---


@pytest.fixture
def sample_articles():
    """Un fixture che fornisce una lista di articoli di esempio."""
    return [
        {
            "title": "Breaking News: Python takes over the world",
            "summary": "A new study shows Python is the most popular language.",
            "url": "http://example.com/python-news",
            "source": "TechCrunch",
        },
        {
            "title": "Data Science with Pandas",
            "summary": "How to use pandas for data analysis.",
            "url": "http://example.com/pandas-tutorial",
            "source": "DataCamp",
        },
        {
            "title": "A guide to JavaScript frameworks",
            "summary": "React vs. Vue vs. Angular.",
            "url": "http://example.com/js-frameworks",
            "source": "SmashingMag",
        },
    ]


@pytest.fixture
def keywords():
    """Un fixture che fornisce una lista di keyword di esempio."""
    return ["Python", "data science"]


@pytest.fixture
def temp_db(tmp_path):
    """
    Un fixture che crea un database SQLite temporaneo per un test.
    Usa il fixture 'tmp_path' di pytest per creare un file in una cartella temporanea.
    """
    db_path = tmp_path / "test_news.db"
    # Inizializza il DB con le tabelle 'articles' e 'logs'
    repo = ArticleRepository(str(db_path))
    repo.initialize_db()
    yield str(db_path)  # Fornisce il percorso al test
    # Nessuna pulizia necessaria, tmp_path viene gestito da pytest


@pytest.fixture
def mock_task_instance():
    """
    Un fixture che simula (mock) l'oggetto TaskInstance di Airflow,
    necessario per il nostro context manager di logging.
    """
    ti = MagicMock()
    ti.dag_id = "test_dag"
    ti.task_id = "test_task"
    return ti


# --- Test per la Logica di Business (Funzioni Pure) ---


def test_filter_articles_by_keywords_match(sample_articles, keywords):
    """Verifica che il filtraggio per keyword funzioni correttamente."""
    filtered = filter_articles_by_keywords(sample_articles, keywords)
    assert len(filtered) == 2
    assert filtered[0]["title"] == "Breaking News: Python takes over the world"
    assert "python" in filtered[0]["matched_keywords"]


def test_filter_articles_no_match(sample_articles):
    """Verifica che non vengano restituiti articoli se nessuna keyword corrisponde."""
    filtered = filter_articles_by_keywords(sample_articles, ["nonexistent", "keyword"])
    assert len(filtered) == 0


def test_generate_telegram_chunks(sample_articles):
    """Verifica che la formattazione per Telegram funzioni."""
    formatter = NotificationFormatter()
    chunks = formatter.generate_telegram_message_chunks(sample_articles, chunk_size=2)
    assert len(chunks) == 2  # 3 articoli, chunk da 2 -> 2 messaggi
    assert "<b>Your Daily News Feed" in chunks[0]  # L'header è nel primo chunk
    assert "<b><a href" in chunks[0]  # Verifica la formattazione HTML
    assert "<b>Your Daily News Feed" not in chunks[1]  # L'header non è nel secondo


# --- Test per le Interazioni con il Database ---


def test_add_articles_and_avoid_duplicates(temp_db, sample_articles):
    """Verifica che l'inserimento nel DB funzioni e che i duplicati vengano ignorati."""
    repo = ArticleRepository(temp_db)
    # Aggiungi un campo necessario dal processo di filtraggio
    for article in sample_articles:
        article["matched_keywords"] = ["test"]
        article["fetch_timestamp"] = "2023-01-01T00:00:00"

    # Primo inserimento
    newly_added = repo.add_articles(sample_articles)
    assert len(newly_added) == 3

    # Secondo inserimento con gli stessi dati
    newly_added_again = repo.add_articles(sample_articles)
    assert len(newly_added_again) == 0  # Nessun nuovo articolo dovrebbe essere aggiunto

    # Verifica lo stato finale del DB
    with sqlite3.connect(temp_db) as conn:
        cursor = conn.cursor()
        count = cursor.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
        assert count == 3


# --- Test per il Sistema di Logging Personalizzato ---


def test_task_db_logger_captures_logs(temp_db, mock_task_instance):
    """
    Verifica che il context manager `task_db_logger` catturi i log
    da qualsiasi logger e li scriva nel database.
    """
    # Logger da un modulo fittizio
    test_logger = logging.getLogger("my.fictional.module")

    with task_db_logger(db_path=temp_db, ti=mock_task_instance):
        test_logger.info("Log di test INFO")
        test_logger.warning("Log di test WARNING")

    # Verifica che i log siano stati scritti nel DB
    with sqlite3.connect(temp_db) as conn:
        cursor = conn.cursor()
        logs = cursor.execute("SELECT level, message, dag_id, task_id FROM logs ORDER BY id").fetchall()

    assert len(logs) == 2
    assert logs[0] == ("INFO", "Log di test INFO", "test_dag", "test_task")
    assert logs[1] == ("WARNING", "Log di test WARNING", "test_dag", "test_task")

