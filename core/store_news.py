"""
Article storage and management module.

This module provides functionalities for storing and managing news articles
in a SQLite database.

Components:
- compute_title_hash: Generate SHA-256 hash from normalized title
- filter_articles_by_keywords: Filter articles based on keyword matching
- ArticleRepository: Database operations handler for articles
"""

import hashlib
import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from typing import Any, Generator, Sequence

logger = logging.getLogger(__name__)


def compute_title_hash(title: str) -> str:
    """Generate a SHA-256 hash from a normalized article title.

    Normalization: lowercase, strip whitespace, collapse internal whitespace.

    Args:
        title: The raw article title.

    Returns:
        Hex-encoded SHA-256 hash string.
    """
    normalized = " ".join(title.lower().split())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


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
    def _get_connection(self) -> Generator[sqlite3.Connection, None, None]:
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

        Creates the articles table with appropriate schema and runs
        any necessary migrations for existing databases.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE NOT NULL,
                    title TEXT NOT NULL,
                    title_hash TEXT UNIQUE,
                    source TEXT,
                    fetch_timestamp TEXT NOT NULL,
                    match_keywords TEXT,
                    ingestion_timestamp TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS article_embeddings (
                    article_id INTEGER PRIMARY KEY,          -- FK verso articles.id
                    embedding   TEXT NOT NULL               -- JSON della lista di float
                )
                """
            )

            # --- Migration: add title_hash column to existing DB ---
            cursor.execute("PRAGMA table_info(articles)")
            existing_columns = {row[1] for row in cursor.fetchall()}
            if "title_hash" not in existing_columns:
                logger.info("Migrating articles table: adding title_hash column")
                cursor.execute("ALTER TABLE articles ADD COLUMN title_hash TEXT")
                cursor.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_articles_title_hash "
                    "ON articles(title_hash)"
                )
                # Backfill hashes for existing rows
                cursor.execute("SELECT id, title FROM articles WHERE title_hash IS NULL")
                for row_id, title in cursor.fetchall():
                    t_hash = compute_title_hash(title)
                    cursor.execute(
                        "UPDATE articles SET title_hash = ? WHERE id = ?",
                        (t_hash, row_id),
                    )
                logger.info("Backfilled title_hash for existing articles")

            logger.info("Database initialized successfully at %s", self.db_path)

    def add_embedding(self, article_id: int, embedding: Sequence[float]) -> None:
        """Add an embedding for an article."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO article_embeddings (article_id, embedding)
                VALUES (?, ?)
                ON CONFLICT(article_id) DO UPDATE SET embedding = excluded.embedding                
                """,
                (article_id, json.dumps(list(embedding))),
            )

    def get_all_embeddings(self) -> list[tuple[int, list[float]]]:
        """
        Retrive url, title, summary to create a list of article
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT article_id, embedding FROM article_embeddings")
            rows = cursor.fetchall()

        result: list[tuple[int, list[float]]] = []
        for article_id, emb_json in rows:
            try:
                emb = json.loads(emb_json)
                result.append((article_id, emb))
            except json.JSONDecodeError:
                logger.warning("Invalid embedding JSON for article_id=%s", article_id)
        return result

    def get_articles_by_ids(self, ids: list[int]) -> list[dict[str, Any]]:
        """Retrieve articles by their IDs.

        Args:
            ids: List of article IDs to retrieve.

        Returns:
            List of article dictionaries.
        """
        if not ids:
            return []
        placeholders = ",".join("?" for _ in ids)
        query = f"""
            SELECT id, url, title, source, fetch_timestamp, match_keywords
            FROM articles
            WHERE id IN ({placeholders})
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, ids)
            rows = cursor.fetchall()

        articles: list[dict[str, Any]] = []
        for row in rows:
            art_id, url, title, source, fetch_ts, match_keywords = row
            articles.append(
                {
                    "id": art_id,
                    "url": url,
                    "title": title,
                    "source": source,
                    "fetch_timestamp": fetch_ts,
                    "matched_keywords": json.loads(match_keywords or "[]"),
                }
            )
        return articles

    def _exists(self, conn: sqlite3.Connection, url: str, title_hash: str) -> bool:
        """Check if an article already exists by URL or title_hash."""
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM articles WHERE url = ? OR title_hash = ? LIMIT 1",
            (url, title_hash),
        )
        return cursor.fetchone() is not None

    def get_known_identifiers(self) -> tuple[set[str], set[str]]:
        """Return sets of known URLs and title_hashes from the database.

        Returns:
            Tuple of (known_urls, known_hashes).
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT url, title_hash FROM articles")
            rows = cursor.fetchall()

        known_urls: set[str] = set()
        known_hashes: set[str] = set()
        for url, t_hash in rows:
            known_urls.add(url)
            if t_hash:
                known_hashes.add(t_hash)
        return known_urls, known_hashes

    def add_articles(self, articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Add articles to the database, skipping duplicates.

        Deduplication is performed on both URL and title_hash.

        Args:
            articles: List of article dictionaries to insert.
        Returns:
            List of successfully inserted articles.
        """
        newly_added_articles = []

        with self._get_connection() as conn:
            cursor = conn.cursor()

            for article in articles:
                url = article["url"]
                title_hash = compute_title_hash(article["title"])

                if self._exists(conn, url, title_hash):
                    logger.debug(
                        "Article already exists (URL: %s, hash: %s), skipping",
                        url,
                        title_hash[:12],
                    )
                    continue

                try:
                    cursor.execute(
                        """
                        INSERT INTO articles
                            (url, title, title_hash, source, fetch_timestamp, match_keywords)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            article["url"],
                            article["title"],
                            title_hash,
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
                except sqlite3.Error as e:
                    logger.error(
                        "Error inserting article '%s' (%s): %s",
                        article.get("title", "Unknown"),
                        article.get("url", "Unknown"),
                        e,
                    )

        return newly_added_articles
