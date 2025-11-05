# pylint: disable=redefined-outer-name
"""
This module contains unit tests for the web scraping Airflow pipeline.

It tests:
'filter_articles_by_keywords' function for correct filtering logic.
'NotificationFormatter' for generating correct Telegram message chunks.
'ArticleRepository' for database interactions, including adding articles and handling duplicates.
'task_db_logger' context manager for capturing and storing logs in the database.
"""

import logging
import sqlite3
from pathlib import Path
from typing import Generator
from unittest.mock import MagicMock
import pytest

from news_feed_pipeline.core.store_news import (
    ArticleRepository,
    filter_articles_by_keywords,
)
from news_feed_pipeline.core.utils import NotificationFormatter
from news_feed_pipeline.core.log_handler import task_db_logger

# --- Fixtures: Mocks ---


@pytest.fixture
def sample_articles() -> list[dict]:
    """A fixture that provides a list of sample articles."""
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
def keywords() -> list[str]:
    """A fixture that provides a list of sample keywords."""
    return ["Python", "data science"]


@pytest.fixture
def temp_db(tmp_path: Path) -> Generator[str, None, None]:
    """
    A fixture that creates a temporary SQLite database for a test.
    It uses pytest's 'tmp_path' fixture to create a file in a temporary directory.
    """
    db_path = tmp_path / "test_news.db"
    repo = ArticleRepository(str(db_path))
    repo.initialize_db()
    yield str(db_path)  # Provide the path to the test
    # No cleanup needed, tmp_path is handled by pytest


@pytest.fixture
def mock_task_instance() -> MagicMock:
    """A fixture that mocks the Airflow TaskInstance object,
    which is necessary for our logging context manager.
    """
    ti = MagicMock()
    ti.dag_id = "test_dag"
    ti.task_id = "test_task"
    return ti


# --- Tests for Business Logic (Pure Functions) ---


def test_filter_articles_by_keywords_match(
    sample_articles: list[dict], keywords: list[str]
) -> None:
    """Tests that keyword filtering works correctly."""
    filtered = filter_articles_by_keywords(sample_articles, keywords)
    assert len(filtered) == 2
    assert filtered[0]["title"] == "Breaking News: Python takes over the world"
    assert "python" in filtered[0]["matched_keywords"]


def test_filter_articles_no_match(sample_articles: list[dict]) -> None:
    """Tests that no articles are returned if no keywords match."""
    filtered = filter_articles_by_keywords(sample_articles, ["nonexistent", "keyword"])
    assert len(filtered) == 0


def test_filter_articles_by_keywords_empty_list(keywords: list[str]) -> None:
    """Tests that an empty list is returned if the input article list is empty."""
    filtered = filter_articles_by_keywords([], keywords)
    assert not filtered


def test_generate_telegram_chunks(sample_articles: list[dict]) -> None:
    """Tests that the Telegram formatting works."""
    formatter = NotificationFormatter()
    chunks = formatter.generate_telegram_message_chunks(sample_articles, chunk_size=2)
    assert len(chunks) == 2  # 3 articles, chunk size 2 -> 2 messages
    assert "<b>Your Daily News Feed" in chunks[0]  # The header is in the first chunk
    assert "<b><a href" in chunks[0]  # Verify HTML formatting
    assert "<b>Your Daily News Feed" not in chunks[1]  # The header is not in the second


# --- Tests for Database Interactions ---


def test_add_articles_and_avoid_duplicates(
    temp_db: str, sample_articles: list[dict]
) -> None:
    """Tests that DB insertion works and duplicates are ignored."""
    repo = ArticleRepository(temp_db)
    # Add a required field from the filtering process
    for article in sample_articles:
        article["matched_keywords"] = ["test"]
        article["fetch_timestamp"] = "2023-01-01T00:00:00"

    # First insertion
    newly_added = repo.add_articles(sample_articles)
    assert len(newly_added) == 3

    # Second insertion with the same data
    newly_added_again = repo.add_articles(sample_articles)
    assert len(newly_added_again) == 0  # No new articles should be added

    # Verify the final state of the DB
    with sqlite3.connect(temp_db) as conn:
        cursor = conn.cursor()
        count = cursor.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
        assert count == 3


# --- Tests for the Custom Logging System ---


def test_task_db_logger_captures_logs(
    temp_db: str, mock_task_instance: MagicMock
) -> None:
    """Tests that the `task_db_logger` context manager captures logs
    from any logger and writes them to the database.
    """
    # Logger from a fictional module
    test_logger = logging.getLogger("my.fictional.module")

    with task_db_logger(db_path=temp_db, ti=mock_task_instance):
        test_logger.info("Test log INFO")
        test_logger.warning("Test log WARNING")

    # Verify that the logs were written to the DB
    with sqlite3.connect(temp_db) as conn:
        cursor = conn.cursor()
        logs = cursor.execute(
            "SELECT level, message, dag_id, task_id FROM logs ORDER BY id"
        ).fetchall()

    assert len(logs) == 2
    assert logs[0] == ("INFO", "Test log INFO", "test_dag", "test_task")
    assert logs[1] == ("WARNING", "Test log WARNING", "test_dag", "test_task")
