"""
Type definitions and Pydantic models for the News Feed Pipeline.
"""

from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, HttpUrl, Field


class NewsSourceConfig(BaseModel):
    """Configuration for a single news source."""
    name: str = Field(..., description="Name of the news source")
    url: str = Field(..., description="URL of the RSS feed")


class Article(BaseModel):
    """Represents a news article."""
    title: str
    url: str
    source: str
    summary: Optional[str] = None
    fetch_timestamp: Optional[str] = None
    matched_keywords: List[str] = Field(default_factory=list)
    
    # Allow extra fields for now to be flexible with RSS data
    class Config:
        extra = "ignore"


class DailyDigestPayload(BaseModel):
    """Payload sent to the daily digest agent."""
    date: str
    articles: List[Article]
