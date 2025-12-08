"""
Article storage and management module.

This module provides functionalities for storing and managing news articles
in a SQLite database.

Components:
- filter_articles_by_keywords: Filter articles based on keyword matching
- ArticleRepository: Database operations handler for articles
"""

import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from typing import Any

logger = logging.getLogger(__name__)


def filter_articles_by_keywords(
    articles: list[dict[str, Any]], keywords: list[str]
) -> list[dict[str, Any]]:
    """Filter articles based on keyword matching in title or summary.

    Args:
        articles: List of article dictionaries to filter.
        keywords: List of keywords to match against.

    Returns:
        Filtered list of articles with 'matched_keywords' field added.
    """
    logger.info("Filtering %d articles with %d keywords", len(articles), len(keywords))
    filtered_articles = []
    lower_keywords = [keyword.lower() for keyword in keywords]

    for article in articles:
        title = article.get("title", "").strip()
        summary = article.get("summary", "").strip()

        if not article.get("url") or not title:
            logger.warning("Skipping article with missing URL or title: %s", article)
            continue

        matched_keywords = {
            keyword
            for keyword in lower_keywords
            if keyword in title.lower() or keyword in summary.lower()
        }

        if matched_keywords:
            article["matched_keywords"] = list(matched_keywords)
            filtered_articles.append(article)
        else:
            logger.debug("Article '%s' did not match any keywords", title)

    logger.info("Found %d articles matching keywords", len(filtered_articles))
    return filtered_articles


class ArticleRepository:
    """Handle all database operations for news articles.
    
    This class provides a clean interface for article storage and retrieval,
    managing database connections and transactions internally.
    """

    def __init__(self, db_path: str) -> None:
        """Initialize the repository with a database path.
        
        Args:
            db_path: Path to the SQLite database file.
        """
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

    @contextmanager
    def _get_connection(self) -> sqlite3.Connection:
        """Provide a transactional database connection.
        
        Yields:
            SQLite connection object.
            
        Raises:
            sqlite3.Error: If database operation fails.
        """
        conn = sqlite3.connect(self.db_path, timeout=15)
        try:
            yield conn
            conn.commit()
        except sqlite3.Error as e:
            logger.error("Database error: %s", e)
            conn.rollback()
            raise
        finally:
            conn.close()

    def initialize_db(self) -> None:
        """Initialize the database and create tables if they don't exist.
        
        Creates the articles table with appropriate schema.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE NOT NULL,
                    title TEXT NOT NULL,
                    source TEXT,
                    fetch_timestamp TEXT NOT NULL,
                    match_keywords TEXT,
                    ingestion_timestamp TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            logger.info("Database initialized successfully at %s", self.db_path)

    def add_articles(self, articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Add articles to the database, skipping duplicates.

        Args:
            articles: List of article dictionaries to insert.

        Returns:
            List of successfully inserted articles.
        """
        newly_added_articles = []
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            for article in articles:
                try:
                    cursor.execute(
                        """
                        INSERT INTO articles (url, title, source, fetch_timestamp, match_keywords)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            article["url"],
                            article["title"],
                            article["source"],
                            article["fetch_timestamp"],
                            json.dumps(article.get("matched_keywords", [])),
                        ),
                    )
                    newly_added_articles.append(article)
                    logger.info(
                        "New article stored: '%s' from %s",
                        article["title"],
                        article["source"],
                    )
                except sqlite3.IntegrityError:
                    logger.debug("Article already exists (URL: %s), skipping", article["url"])
                except sqlite3.Error as e:
                    logger.error(
                        "Error inserting article '%s' (%s): %s",
                        article.get("title", "Unknown"),
                        article.get("url", "Unknown"),
                        e,
                    )
        
        return newly_added_articles
