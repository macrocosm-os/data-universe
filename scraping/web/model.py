import datetime as dt
from typing import Dict, List, Optional, Any
import json

# Use v1 for these models to keep serialization consistent.
# Pydantic v2 doesn't include spaces in its serialization.
from pydantic.v1 import BaseModel, Field, validator

from common import constants
from common.data import DataEntity, DataLabel, DataSource
from scraping import utils


class WebContent(BaseModel):
    """The content model for web search results.

    The model helps standardize the data format for web content scraped via Firecrawl.
    """

    class Config:
        extra = "forbid"

    # model_config should NOT be set by Miners.
    # In the near future, Validators will penalized Miners who set this field.
    model_config: Dict[str, str] = Field(default=None)

    url: str = Field(description="The URL of the web page")
    title: str = Field(description="The title of the web page")
    content: str = Field(description="The main content of the web page")
    timestamp: dt.datetime = Field(description="When the content was scraped")
    
    # Optional metadata fields
    description: Optional[str] = Field(
        default=None,
        description="Meta description of the page"
    )
    keywords: Optional[List[str]] = Field(
        default_factory=list,
        description="Keywords/tags associated with the content"
    )
    author: Optional[str] = Field(
        default=None,
        description="Author of the content if available"
    )
    language: Optional[str] = Field(
        default=None,
        description="Language of the content"
    )
    
    # Firecrawl specific metadata
    content_type: Optional[str] = Field(
        default=None,
        description="Type of content (article, blog, etc.)"
    )
    word_count: Optional[int] = Field(
        default=None,
        description="Word count of the content"
    )
    images: Optional[List[str]] = Field(
        default_factory=list,
        description="List of image URLs found in the content"
    )
    links: Optional[List[str]] = Field(
        default_factory=list,
        description="List of external links found in the content"
    )
    
    # Search context
    search_query: Optional[str] = Field(
        default=None,
        description="The search query that led to this result (if applicable)"
    )
    search_rank: Optional[int] = Field(
        default=None,
        description="The rank/position in search results (if applicable)"
    )

    @validator("url")
    def url_must_be_valid(cls, v):
        """Validate that the URL is properly formatted."""
        if not v or not v.startswith(("http://", "https://")):
            raise ValueError("URL must start with http:// or https://")
        return v

    @validator("content")
    def content_must_not_be_empty(cls, v):
        """Validate that content is not empty."""
        if not v or not v.strip():
            raise ValueError("Content cannot be empty")
        return v

    @validator("title")
    def title_must_not_be_empty(cls, v):
        """Validate that title is not empty."""
        if not v or not v.strip():
            raise ValueError("Title cannot be empty")
        return v

    @validator("timestamp")
    def timestamp_must_be_utc(cls, v):
        """Ensure timestamp is in UTC."""
        if v.tzinfo is None:
            v = v.replace(tzinfo=dt.timezone.utc)
        elif v.tzinfo != dt.timezone.utc:
            v = v.astimezone(dt.timezone.utc)
        return v

    def to_data_entity(self) -> DataEntity:
        """Convert the WebContent to a DataEntity."""
        content_dict = {
            "url": self.url,
            "title": self.title,
            "content": self.content,
            "description": self.description,
            "keywords": self.keywords,
            "author": self.author,
            "language": self.language,
            "content_type": self.content_type,
            "word_count": self.word_count,
            "images": self.images,
            "links": self.links,
            "search_query": self.search_query,
            "search_rank": self.search_rank,
            "scraped_at": self.timestamp.isoformat()
        }

        # Remove None values to keep JSON clean
        content_dict = {k: v for k, v in content_dict.items() if v is not None}

        # Convert to JSON bytes
        content_bytes = json.dumps(content_dict, ensure_ascii=False).encode('utf-8')

        # Create label from domain or search query
        label_value = self._create_label()

        return DataEntity(
            uri=self.url,
            datetime=self.timestamp,
            source=DataSource.WEB_SEARCH,
            label=DataLabel(value=label_value),
            content=content_bytes,
            content_size_bytes=len(content_bytes)
        )

    def _create_label(self) -> str:
        """Create a label for the web content."""
        if self.search_query:
            # Use search query as label (sanitized)
            return utils.sanitize_label(self.search_query)
        
        # Extract domain from URL as fallback
        try:
            from urllib.parse import urlparse
            domain = urlparse(self.url).netloc
            if domain.startswith('www.'):
                domain = domain[4:]
            return utils.sanitize_label(domain)
        except Exception:
            return "web_content"

    @classmethod
    def from_data_entity(cls, entity: DataEntity) -> "WebContent":
        """Create a WebContent from a DataEntity."""
        if entity.source != DataSource.WEB_SEARCH:
            raise ValueError(f"Expected WEB_SEARCH source, got {entity.source}")

        try:
            content_str = entity.content.decode("utf-8")
            content_dict = json.loads(content_str)
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise ValueError(f"Failed to decode content: {e}")

        # Extract timestamp from content or use entity datetime
        timestamp = entity.datetime
        if "scraped_at" in content_dict:
            try:
                timestamp = dt.datetime.fromisoformat(content_dict["scraped_at"].replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                pass

        return cls(
            url=content_dict.get("url", entity.uri),
            title=content_dict.get("title", ""),
            content=content_dict.get("content", ""),
            timestamp=timestamp,
            description=content_dict.get("description"),
            keywords=content_dict.get("keywords", []),
            author=content_dict.get("author"),
            language=content_dict.get("language"),
            content_type=content_dict.get("content_type"),
            word_count=content_dict.get("word_count"),
            images=content_dict.get("images", []),
            links=content_dict.get("links", []),
            search_query=content_dict.get("search_query"),
            search_rank=content_dict.get("search_rank")
        )

    @classmethod
    def create_search_label(cls, query: str) -> str:
        """Create a standardized label for search queries."""
        return f"search_{utils.sanitize_label(query)}"

    @classmethod
    def create_domain_label(cls, url: str) -> str:
        """Create a standardized label for domains."""
        try:
            from urllib.parse import urlparse
            domain = urlparse(url).netloc
            if domain.startswith('www.'):
                domain = domain[4:]
            return f"domain_{utils.sanitize_label(domain)}"
        except Exception:
            return "web_content"



class WebSearchMetrics(BaseModel):
    """Metrics for web search operations."""
    
    query: str = Field(description="The search query")
    results_count: int = Field(description="Number of results returned")
    total_content_size: int = Field(description="Total size of content in bytes")
    average_response_time: float = Field(description="Average response time in seconds")
    success_rate: float = Field(description="Success rate (0.0 to 1.0)")
    timestamp: dt.datetime = Field(description="When the metrics were collected")
    
    @validator("success_rate")
    def validate_success_rate(cls, v):
        """Validate success rate is between 0 and 1."""
        if not 0.0 <= v <= 1.0:
            raise ValueError("Success rate must be between 0.0 and 1.0")
        return v