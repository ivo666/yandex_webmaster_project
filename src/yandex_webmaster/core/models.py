"""Data models for Yandex Webmaster."""
from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class RawSearchQuery(BaseModel):
    """Raw data model for search queries from Yandex Webmaster."""
    query_id: str = Field(..., description="Unique query identifier")
    query_text: str = Field(..., description="Search query text")
    page_url: str = Field(..., description="Page URL")
    position: Optional[float] = Field(None, description="Average position")
    impressions: int = Field(..., description="Number of impressions")
    clicks: int = Field(..., description="Number of clicks")
    ctr: Optional[float] = Field(None, description="Click-through rate")
    date: datetime = Field(..., description="Date of statistics")
    raw_json: Dict[str, Any] = Field(default_factory=dict, description="Raw API response")
    
    class Config:
        from_attributes = True


class RawSitemapInfo(BaseModel):
    """Raw data model for sitemap information."""
    sitemap_id: str
    url: str
    added_date: datetime
    last_access_date: Optional[datetime]
    errors_count: Optional[int]
    urls_count: Optional[int]
    raw_json: Dict[str, Any] = Field(default_factory=dict)