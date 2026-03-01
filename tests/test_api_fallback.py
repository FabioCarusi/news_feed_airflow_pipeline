
import pytest
from unittest.mock import MagicMock, patch
from openai import RateLimitError, APIStatusError, NotFoundError
from news_feed_pipeline.core.daily_digest_agent import DailyDigestAgent

@pytest.fixture
def agent():
    return DailyDigestAgent(api_key="test_key", model_name="test_model")

@pytest.fixture
def sample_payload():
    return {
        "date": "05 February 2026",
        "articles": [
            {"title": "Article 1", "url": "http://test.com/1", "summary": "Summary 1"},
            {"title": "Article 2", "url": "http://test.com/2", "summary": "Summary 2"}
        ]
    }

def test_rate_limit_fallback(agent, sample_payload):
    """Test that RateLimitError triggers the fallback digest."""
    with patch.object(agent, "client") as mock_client:
        # Configure mock to raise RateLimitError
        mock_response = MagicMock()
        mock_client.chat.completions.create.side_effect = RateLimitError(
            message="Rate limit exceeded",
            response=mock_response,
            body={"error": {"message": "Rate limit exceeded"}}
        )

        result = agent._call_openai_agent(sample_payload)

        # Assertions
        assert "Fallback Mode" in result
        assert "http://test.com/1" in result
        assert "Article 1" in result
        assert "http://test.com/2" in result
        assert "Link diretti" in result or "link diretti" in result

def test_api_status_error_403_fallback(agent, sample_payload):
    """Test that APIStatusError with 403 (Forbidden) triggers the fallback digest."""
    with patch.object(agent, "client") as mock_client:
        # Configure mock to raise APIStatusError 403
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_client.chat.completions.create.side_effect = APIStatusError(
             message="Forbidden",
             response=mock_response,
             body={"error": {"message": "Forbidden"}}
        )

        result = agent._call_openai_agent(sample_payload)

        # Assertions
        assert "Fallback Mode" in result
        assert "http://test.com/1" in result

def test_not_found_error_retries_fallback_model(agent, sample_payload):
    """Test that NotFoundError (404) retries with the fallback model."""
    with patch.object(agent, "client") as mock_client:
        # First call raises NotFoundError
        # Second call returns success
        mock_response_success = MagicMock()
        mock_response_success.choices[0].message.content = "Success from fallback"
        mock_response_success.choices[0].message.tool_calls = None
        
        mock_response_404 = MagicMock()
        mock_response_404.status_code = 404

        def side_effect(*args, **kwargs):
            if kwargs["model"] == "test_model":
                 raise NotFoundError(
                     message="Model not found",
                     response=mock_response_404,
                     body={"error": {"message": "Model not found"}}
                 )
            return mock_response_success

        mock_client.chat.completions.create.side_effect = side_effect

        result = agent._call_openai_agent(sample_payload)
        
        assert result == "Success from fallback"
        
        # Verify calls
        assert mock_client.chat.completions.create.call_count == 2
        call_args_list = mock_client.chat.completions.create.call_args_list
        assert call_args_list[0].kwargs["model"] == "test_model"
        assert call_args_list[1].kwargs["model"] == "openai/gpt-oss-20b:free"
