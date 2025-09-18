"""
Firecrawl-based web search and content extraction scraper for Data Universe subnet.
"""

import asyncio
import json
import logging
import time
from typing import List, Dict, Optional, Any
from datetime import datetime, timezone
import bittensor as bt
from firecrawl import Firecrawl
from common.data import DataEntity, DataSource, DataLabel, TimeBucket
from common.protocol import OnDemandRequest
from scraping.scraper import ScraperInterface
from scraping.provider import ScraperProvider


class FirecrawlWebSearchResult:
    """Represents a web search result from Firecrawl"""
    
    def __init__(self, url: str, title: str, content: str, metadata: Dict = None):
        self.url = url
        self.title = title
        self.content = content
        self.metadata = metadata or {}
        self.timestamp = datetime.now(timezone.utc)


class FirecrawlWebScraper(ScraperInterface):
    """Firecrawl-based web scraper for searching and extracting web content"""
    
    def __init__(self, api_key: str = None, max_retries: int = 3):
        """
        Initialize Firecrawl web scraper
        
        Args:
            api_key: Firecrawl API key (if None, will try to get from environment)
            max_retries: Maximum number of retries for failed requests
        """
        try:
            self.firecrawl = Firecrawl(api_key=api_key)
            self.max_retries = max_retries
            bt.logging.info("Firecrawl web scraper initialized successfully")
        except Exception as e:
            bt.logging.error(f"Failed to initialize Firecrawl: {e}")
            raise
    
    async def search_and_crawl(self, query: str, limit: int = 10, formats: List[str] = None) -> List[FirecrawlWebSearchResult]:
        """
        Search the web and crawl results using Firecrawl
        
        Args:
            query: Search query string
            limit: Maximum number of results to return
            formats: List of formats to extract (markdown, html, structured_data)
            
        Returns:
            List of FirecrawlWebSearchResult objects
        """
        if formats is None:
            formats = ["markdown"]
            
        results = []
        
        try:
            # Since Firecrawl doesn't have direct search capability, we'll use common search engines
            # and then crawl the results. For now, we'll simulate search by crawling known URLs
            # In a real implementation, you might integrate with Google Search API or similar
            
            bt.logging.info(f"Performing web search for query: {query}")
            
            # For demonstration, we'll crawl a few general URLs related to the query
            # In production, you'd want to integrate with a search API first
            search_urls = self._generate_search_urls(query)
            
            for url in search_urls[:limit]:
                try:
                    result = await self._crawl_url(url, formats)
                    if result:
                        results.append(result)
                except Exception as e:
                    bt.logging.warning(f"Failed to crawl {url}: {e}")
                    continue
                    
        except Exception as e:
            bt.logging.error(f"Error in search_and_crawl: {e}")
            
        return results[:limit]
    
    async def crawl_urls(self, urls: List[str], formats: List[str] = None) -> List[FirecrawlWebSearchResult]:
        """
        Crawl specific URLs using Firecrawl
        
        Args:
            urls: List of URLs to crawl
            formats: List of formats to extract
            
        Returns:
            List of FirecrawlWebSearchResult objects
        """
        if formats is None:
            formats = ["markdown"]
            
        results = []
        
        for url in urls:
            try:
                result = await self._crawl_url(url, formats)
                if result:
                    results.append(result)
            except Exception as e:
                bt.logging.warning(f"Failed to crawl {url}: {e}")
                continue
                
        return results
    
    async def _crawl_url(self, url: str, formats: List[str]) -> Optional[FirecrawlWebSearchResult]:
        """
        Crawl a single URL using Firecrawl
        
        Args:
            url: URL to crawl
            formats: List of formats to extract
            
        Returns:
            FirecrawlWebSearchResult or None if failed
        """
        for attempt in range(self.max_retries):
            try:
                # Use asyncio to run the blocking Firecrawl call
                loop = asyncio.get_event_loop()
                scrape_result = await loop.run_in_executor(
                    None, 
                    lambda: self.firecrawl.scrape(url, formats=formats)
                )
                
                if scrape_result:
                    # Extract content based on requested formats
                    content = ""
                    metadata = {}
                    
                    if "markdown" in formats and "markdown" in scrape_result:
                        content = scrape_result["markdown"]
                    elif "html" in formats and "html" in scrape_result:
                        content = scrape_result["html"]
                    elif scrape_result.get("content"):
                        content = scrape_result["content"]
                    
                    # Extract metadata
                    if "metadata" in scrape_result:
                        metadata = scrape_result["metadata"]
                    
                    title = metadata.get("title", "")
                    if not title and "ogTitle" in metadata:
                        title = metadata["ogTitle"]
                    
                    return FirecrawlWebSearchResult(
                        url=url,
                        title=title,
                        content=content,
                        metadata=metadata
                    )
                    
            except Exception as e:
                bt.logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(1 * (attempt + 1))  # Exponential backoff
                continue
                
        return None
    
    def _generate_search_urls(self, query: str) -> List[str]:
        """
        Generate URLs to search based on query
        This is a simplified implementation - in production you'd use a search API
        
        Args:
            query: Search query
            
        Returns:
            List of URLs to crawl
        """
        # This is a placeholder implementation
        # In a real scenario, you'd use Google Search API, Bing API, etc.
        base_urls = [
            f"https://www.google.com/search?q={query.replace(' ', '+')}",
            f"https://www.bing.com/search?q={query.replace(' ', '+')}",
            f"https://duckduckgo.com/?q={query.replace(' ', '+')}",
        ]
        
        # For demonstration, return some general URLs
        # In production, parse search results to get actual content URLs
        return base_urls[:3]
    
    def results_to_data_entities(self, results: List[FirecrawlWebSearchResult], 
                                time_bucket: TimeBucket) -> List[DataEntity]:
        """
        Convert Firecrawl results to DataEntity objects
        
        Args:
            results: List of FirecrawlWebSearchResult objects
            time_bucket: TimeBucket for the data
            
        Returns:
            List of DataEntity objects
        """
        entities = []
        
        for result in results:
            try:
                # Create content dictionary
                content_dict = {
                    "title": result.title,
                    "content": result.content,
                    "url": result.url,
                    "metadata": result.metadata,
                    "scraped_at": result.timestamp.isoformat()
                }
                
                # Convert to JSON bytes
                content_bytes = json.dumps(content_dict).encode('utf-8')
                
                # Create DataEntity
                entity = DataEntity(
                    uri=result.url,
                    datetime=result.timestamp,
                    source=DataSource.WEB_SEARCH,
                    label=DataLabel(value="web_search"),
                    content=content_bytes,
                    content_size_bytes=len(content_bytes)
                )
                
                entities.append(entity)
                
            except Exception as e:
                bt.logging.error(f"Failed to convert result to DataEntity: {e}")
                continue
                
        return entities


class FirecrawlScraperProvider(ScraperProvider):
    """Provider for Firecrawl-based web scraping"""
    
    def __init__(self, api_key: str = None):
        """
        Initialize Firecrawl scraper provider
        
        Args:
            api_key: Firecrawl API key
        """
        self.api_key = api_key
        self.scraper = None
    
    def get_scraper(self) -> FirecrawlWebScraper:
        """Get or create Firecrawl scraper instance"""
        if self.scraper is None:
            self.scraper = FirecrawlWebScraper(api_key=self.api_key)
        return self.scraper
    
    async def handle_web_search_request(self, request: OnDemandRequest) -> List[DataEntity]:
        """
        Handle web search OnDemandRequest
        
        Args:
            request: OnDemandRequest with web search parameters
            
        Returns:
            List of DataEntity objects
        """
        scraper = self.get_scraper()
        
        # Create time bucket for current time
        time_bucket = TimeBucket.from_datetime(datetime.now(timezone.utc))
        
        results = []
        
        # Handle web search query
        if request.web_search_query:
            search_results = await scraper.search_and_crawl(
                query=request.web_search_query,
                limit=request.limit,
                formats=request.formats or ["markdown"]
            )
            results.extend(search_results)
        
        # Handle specific URLs
        if request.urls:
            crawl_results = await scraper.crawl_urls(
                urls=request.urls,
                formats=request.formats or ["markdown"]
            )
            results.extend(crawl_results)
        
        # Convert to DataEntity objects
        entities = scraper.results_to_data_entities(results, time_bucket)
        
        bt.logging.info(f"Firecrawl web search completed: {len(entities)} entities created")
        
        return entities