# pylint: disable=redefined-outer-name
"""
Unit tests for the news feed pipeline.

This module tests:
- filter_articles_by_keywords: Keyword filtering logic
- NotificationFormatter: Telegram message chunk generation
- ArticleRepository: Database operations including article insertion and duplicate handling
"""

import sqlite3
from pathlib import Path
from typing import Any, Generator

import pytest

from news_feed_pipeline.core.store_news import (
    ArticleRepository,
    filter_articles_by_keywords,
)
from news_feed_pipeline.core.utils import NotificationFormatter


# --- Fixtures ---


@pytest.fixture
def sample_articles() -> list[dict[str, Any]]:
    """Provide a list of sample articles for testing."""
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
    """Provide a list of sample keywords for testing."""
    return ["Python", "data science"]


@pytest.fixture
def temp_db(tmp_path: Path) -> Generator[str, None, None]:
    """Create a temporary SQLite database for testing.
    
    Uses pytest's tmp_path fixture to create a file in a temporary directory.
    The database is automatically cleaned up after the test.
    """
    db_path = tmp_path / "test_news.db"
    repo = ArticleRepository(str(db_path))
    repo.initialize_db()
    yield str(db_path)
    # Cleanup is handled automatically by pytest's tmp_path


# --- Tests for Business Logic ---


def test_filter_articles_by_keywords_match(
    sample_articles: list[dict[str, Any]], keywords: list[str]
) -> None:
    """Test that keyword filtering correctly identifies matching articles."""
    filtered = filter_articles_by_keywords(sample_articles, keywords)
    assert len(filtered) == 2
    assert filtered[0]["title"] == "Breaking News: Python takes over the world"
    assert "python" in filtered[0]["matched_keywords"]


def test_filter_articles_no_match(sample_articles: list[dict[str, Any]]) -> None:
    """Test that no articles are returned when no keywords match."""
    filtered = filter_articles_by_keywords(sample_articles, ["nonexistent", "keyword"])
    assert len(filtered) == 0


def test_filter_articles_by_keywords_empty_list(keywords: list[str]) -> None:
    """Test that an empty list is returned when input article list is empty."""
    filtered = filter_articles_by_keywords([], keywords)
    assert not filtered


def test_generate_telegram_chunks(sample_articles: list[dict[str, Any]]) -> None:
    """Test that Telegram message formatting works correctly."""
    formatter = NotificationFormatter()
    chunks = formatter.generate_telegram_message_chunks(sample_articles, chunk_size=2)
    assert len(chunks) == 2  # 3 articles with chunk size 2 -> 2 messages
    assert "<b>Your Daily News Feed" in chunks[0]  # Header in first chunk
    assert "<b><a href" in chunks[0]  # Verify HTML formatting
    assert "<b>Your Daily News Feed" not in chunks[1]  # Header not in second chunk


# --- Tests for Database Operations ---


def test_add_articles_and_avoid_duplicates(
    temp_db: str, sample_articles: list[dict[str, Any]]
) -> None:
    """Test that database insertion works and duplicates are properly ignored."""
    repo = ArticleRepository(temp_db)
    
    # Add required fields from the filtering process
    for article in sample_articles:
        article["matched_keywords"] = ["test"]
        article["fetch_timestamp"] = "2023-01-01T00:00:00"

    # First insertion
    newly_added = repo.add_articles(sample_articles)
    assert len(newly_added) == 3

    # Second insertion with the same data
    newly_added_again = repo.add_articles(sample_articles)
    assert len(newly_added_again) == 0  # No new articles should be added

    # Verify the final state of the database
    with sqlite3.connect(temp_db) as conn:
        cursor = conn.cursor()
        count = cursor.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
        assert count == 3
