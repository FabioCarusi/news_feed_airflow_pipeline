# pylint: disable=redefined-outer-name
"""
Unit tests for the fetch_rss_news module.

This module tests the fetch_rss_articles function, ensuring it correctly handles
successful fetches, error responses, and exceptions.
"""

from unittest.mock import patch, MagicMock
import pytest
from news_feed_pipeline.core.fetch_rss_news import fetch_rss_articles

# --- Fixtures ---

@pytest.fixture
def mock_feed_entries() -> list[dict]:
    """Provides a list of sample feed entries for mocking."""
    return [
        {
            "title": "Test Article 1",
            "link": "http://example.com/article1",
            "summary": "Summary of article 1.",
        },
        {
            "title": "Test Article 2",
            "link": "http://example.com/article2",
            "summary": "Summary of article 2.",
        },
    ]


# --- Tests for fetch_rss_articles ---


@patch("webscraping_airflow_pipeline.include.fetch_rss_news.feedparser.parse")
def test_fetch_rss_articles_success(
    mock_parse: MagicMock, mock_feed_entries: list[dict]
) -> None:
    """
    Tests the successful fetching and parsing of articles from an RSS feed.
    """
    # --- Arrange: Mock the feedparser response for a successful case ---
    mock_feed = MagicMock()
    mock_feed.status = 200
    mock_feed.entries = mock_feed_entries
    mock_parse.return_value = mock_feed

    # --- Act: Call the function under test ---
    articles = fetch_rss_articles("http://fake-rss-url.com", "Test Source")

    # --- Assert: Verify the results ---
    assert len(articles) == 2
    assert articles[0]["title"] == "Test Article 1"
    assert articles[0]["source"] == "Test Source"
    assert articles[1]["url"] == "http://example.com/article2"
    mock_parse.assert_called_once_with("http://fake-rss-url.com")


@patch("webscraping_airflow_pipeline.include.fetch_rss_news.feedparser.parse")
def test_fetch_rss_articles_http_error(mock_parse: MagicMock) -> None:
    """
    Tests that an empty list is returned when the feed status is not 200.
    """
    # --- Arrange: Mock a non-200 status code ---
    mock_feed = MagicMock()
    mock_feed.status = 404
    mock_parse.return_value = mock_feed

    # --- Act: Call the function ---
    articles = fetch_rss_articles("http://fake-rss-url.com", "Test Source")

    # --- Assert: Verify that an empty list is returned ---
    assert not articles


@patch("webscraping_airflow_pipeline.include.fetch_rss_news.feedparser.parse")
def test_fetch_rss_articles_empty_feed(mock_parse: MagicMock) -> None:
    """
    Tests that an empty list is returned for a valid feed with no entries.
    """
    # --- Arrange: Mock a 200 status but with empty entries ---
    mock_feed = MagicMock()
    mock_feed.status = 200
    mock_feed.entries = []
    mock_parse.return_value = mock_feed

    # --- Act: Call the function ---
    articles = fetch_rss_articles("http://fake-rss-url.com", "Test Source")

    # --- Assert: Verify that an empty list is returned ---
    assert not articles


@patch("webscraping_airflow_pipeline.include.fetch_rss_news.feedparser.parse")
def test_fetch_rss_articles_exception(mock_parse: MagicMock) -> None:
    """
    Tests that exceptions during fetching are caught and re-raised.
    """
    # --- Arrange: Mock an exception during parsing ---
    mock_parse.side_effect = ConnectionError("Failed to connect")

    # --- Act & Assert: Verify that the exception is raised ---
    with pytest.raises(ConnectionError, match="Failed to connect"):
        fetch_rss_articles("http://fake-rss-url.com", "Test Source")
