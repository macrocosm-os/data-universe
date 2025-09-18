import bittensor as bt
from typing import List, Dict, Any, Optional
from common.data import DataSource
from common.protocol import KeywordMode


class OrganicRequest(bt.Synapse):
    """Direct query synapse for organic data requests"""

    # Input fields
    source: str
    usernames: List[str] = []
    keywords: List[str] = []
    keyword_mode: KeywordMode = "all"
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    limit: int = 100
    
    # Web search specific parameters
    web_search_query: Optional[str] = None
    urls: List[str] = []
    crawl_limit: Optional[int] = 10
    include_metadata: Optional[bool] = True
    formats: List[str] = ["markdown"]
    extract_schema: Optional[str] = None

    # Output fields
    data: List[Dict[str, Any]] = []
    meta: Dict[str, Any] = {}
    status: str = "pending"

    def deserialize(self) -> Dict[str, Any]:
        """Convert synapse to dictionary for response"""
        return {
            "status": self.status,
            "data": self.data,
            "meta": self.meta
        }