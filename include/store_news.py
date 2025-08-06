import sqlite3
import os
import logging
import json
from contextlib import contextmanager

dag_logger = logging.getLogger(__name__)


def filter_articles_by_keywords(
    articles: list[dict], keywords: list[str]
) -> list[dict]:
    """
    Filters a list of articles based on a list of keywords.

    This is pure business logic, decoupled from the database.

    Args:
        articles (list[dict]): The list of articles to filter.
        keywords (list[str]): The keywords to match against.

    Returns:
        list[dict]: A new list containing only the articles that matched,
                    with an added 'matched_keywords' key.
    """
    dag_logger.info(f"Filtering {len(articles)} articles with {len(keywords)} keywords.")
    filtered_articles = []
    lower_keywords = [k.lower() for k in keywords]

    for article in articles:
        title = article.get("title", "").strip()
        summary = article.get("summary", "").strip()

        if not article.get("url") or not title:
            dag_logger.warning(f"Skipping article with missing URL or Title: {article}")
            continue

        matched_keywords = {
            kw for kw in lower_keywords if kw in title.lower() or kw in summary.lower()
        }

        if matched_keywords:
            article["matched_keywords"] = list(matched_keywords)
            filtered_articles.append(article)
        else:
            dag_logger.debug(f"Article '{title}' did not match any keywords. Skipping.")

    dag_logger.info(f"Found {len(filtered_articles)} articles matching keywords.")
    return filtered_articles


class ArticleRepository:
    """
    Handles all database operations for articles.
    This class is the single source of truth for database interactions.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

    @contextmanager
    def _get_connection(self):
        """Provides a transactional database connection."""
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        except sqlite3.Error as e:
            dag_logger.error(f"Database error: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    def initialize_db(self):
        """Initializes the database and creates the articles table if it doesn't exist."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE NOT NULL,
                    title TEXT NOT NULL,
                    source TEXT,
                    fetch_timestamp TEXT NOT NULL,
                    match_keywords TEXT,
                    ingestion_timestamp TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
        dag_logger.info(f"Database initialized and table 'articles' verified at {self.db_path}")

    def add_articles(self, articles: list[dict]) -> list[dict]:
        """
        Adds a list of articles to the database, skipping duplicates.

        Returns:
            list[dict]: A list of articles that were newly inserted.
        """
        newly_added_articles = []
        with self._get_connection() as conn:
            cursor = conn.cursor()
            for article in articles:
                try:
                    cursor.execute("""
                        INSERT INTO articles (url, title, source, fetch_timestamp, match_keywords)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        article["url"], article["title"], article["source"],
                        article["fetch_timestamp"], json.dumps(article["matched_keywords"])
                    ))
                    newly_added_articles.append(article)
                    dag_logger.info(f"New article stored: '{article['title']}' from {article['source']}")
                except sqlite3.IntegrityError:
                    dag_logger.debug(f"Article already exists (URL: {article['url']}). Skipping.")
                except Exception as e:
                    dag_logger.error(f"Error inserting article '{article['title']}' ({article['url']}): {e}")

            conn.commit()
        return newly_added_articles
