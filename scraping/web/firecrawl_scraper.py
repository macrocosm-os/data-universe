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
from scraping.scraper import Scraper, ValidationResult
from scraping.provider import ScraperProvider
from scraping.web.model import WebContent, WebSearchMetrics
from scraping.web import utils


class FirecrawlWebScraper(Scraper):
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
    
    async def search_and_crawl(self, query: str, limit: int = 10, formats: List[str] = None) -> List[WebContent]:
        """
        Search the web using Firecrawl's search API and extract content
        
        Args:
            query: Search query string
            limit: Maximum number of results to return
            formats: List of formats to extract (markdown, html, structured_data)
            
        Returns:
            List of WebContent objects
        """
        if formats is None:
            formats = ["markdown"]
            
        results = []
        
        try:
            bt.logging.info(f"Performing Firecrawl web search for query: {query}")
            
            # Use Firecrawl's search API with content extraction
            loop = asyncio.get_event_loop()
            search_result = await loop.run_in_executor(
                None,
                lambda: self.firecrawl.search(
                    query=query,
                    limit=limit,
                    scrape_options={'formats': formats}
                )
            )
            
            if search_result and 'data' in search_result:
                for item in search_result['data'][:limit]:
                    try:
                        # Extract content from search result
                        url = item.get('url', '')
                        metadata = item.get('metadata', {})
                        title = metadata.get('title', '') or item.get('title', '')
                        
                        # Get content based on requested formats
                        content = ""
                        if "markdown" in formats and "markdown" in item:
                            content = item["markdown"]
                        elif "html" in formats and "html" in item:
                            content = item["html"]
                        elif item.get("content"):
                            content = item["content"]
                        
                        if url and content:
                            # Create WebContent object
                            web_content = WebContent(
                                url=url,
                                title=title,
                                content=content,
                                timestamp=datetime.now(timezone.utc),
                                description=metadata.get('description'),
                                keywords=metadata.get('keywords', []),
                                author=metadata.get('author'),
                                language=metadata.get('language'),
                                content_type=metadata.get('content_type'),
                                word_count=len(content.split()) if content else 0,
                                images=metadata.get('images', []),
                                links=metadata.get('links', []),
                                search_query=query,
                                search_rank=len(results) + 1
                            )
                            
                            # Add the content (validation will be done by validators)
                            results.append(web_content)
                            
                    except Exception as e:
                        bt.logging.warning(f"Failed to process search result: {e}")
                        continue
            else:
                bt.logging.warning("No search results returned from Firecrawl")
                    
        except Exception as e:
            bt.logging.error(f"Error in Firecrawl search: {e}")
            
        return results[:limit]
    
    async def crawl_urls(self, urls: List[str], formats: List[str] = None) -> List[WebContent]:
        """
        Crawl specific URLs using Firecrawl
        
        Args:
            urls: List of URLs to crawl
            formats: List of formats to extract
            
        Returns:
            List of WebContent objects
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
    
    async def _crawl_url(self, url: str, formats: List[str]) -> Optional[WebContent]:
        """
        Crawl a single URL using Firecrawl
        
        Args:
            url: URL to crawl
            formats: List of formats to extract
            
        Returns:
            WebContent or None if failed
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
    
    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        """Validate web search DataEntities by re-scraping URLs and comparing content"""
        if not entities:
            return []
        
        # Randomly sample entities for validation (similar to SN22 approach)
        entities_to_validate = utils.validate_random_url_subset(entities, sample_ratio=0.5)
        bt.logging.info(f"Validating {len(entities_to_validate)} out of {len(entities)} web entities")
        
        results = []
        
        for entity in entities_to_validate:
            try:
                # Validate entity source
                if entity.source != DataSource.WEB_SEARCH:
                    results.append(ValidationResult(
                        is_valid=False,
                        reason="Entity is not from WEB_SEARCH source",
                        content_size_bytes_validated=entity.content_size_bytes
                    ))
                    continue
                
                # Parse miner's submitted content
                try:
                    miner_content = WebContent.from_data_entity(entity)
                except Exception as e:
                    results.append(ValidationResult(
                        is_valid=False,
                        reason=f"Failed to parse WebContent: {str(e)}",
                        content_size_bytes_validated=entity.content_size_bytes
                    ))
                    continue
                
                # Validate URL format
                if not utils.is_valid_web_url(miner_content.url):
                    results.append(ValidationResult(
                        is_valid=False,
                        reason="Invalid URL format",
                        content_size_bytes_validated=entity.content_size_bytes
                    ))
                    continue
                
                # Re-scrape the URL to get validator's version
                try:
                    validator_content = await self._crawl_url(miner_content.url, ["markdown"])
                    
                    if validator_content is None:
                        results.append(ValidationResult(
                            is_valid=False,
                            reason="Failed to re-scrape URL for validation",
                            content_size_bytes_validated=entity.content_size_bytes
                        ))
                        continue
                    
                    # Use utils validation with re-scraped content
                    validation_result = utils.validate_web_content_with_rescrape(
                        miner_content=miner_content,
                        validator_scraped_content=validator_content,
                        entity=entity
                    )
                    
                    results.append(validation_result)
                    
                except Exception as e:
                    results.append(ValidationResult(
                        is_valid=False,
                        reason=f"Validation error during re-scrape: {str(e)}",
                        content_size_bytes_validated=entity.content_size_bytes
                    ))
                    
            except Exception as e:
                bt.logging.error(f"Error validating entity: {e}")
                results.append(ValidationResult(
                    is_valid=False,
                    reason=f"Validation exception: {str(e)}",
                    content_size_bytes_validated=entity.content_size_bytes if entity else 0
                ))
        
        # For entities not validated, mark as valid (similar to SN22 sampling approach)
        non_validated_count = len(entities) - len(entities_to_validate)
        for _ in range(non_validated_count):
            results.append(ValidationResult(
                is_valid=True,
                reason="Not selected for validation sampling",
                content_size_bytes_validated=0
            ))
        
        return results
    
    async def scrape(self, scrape_config) -> List[DataEntity]:
        """Scrape method required by Scraper interface - not used for web search OnDemand"""
        bt.logging.warning("FirecrawlWebScraper.scrape() called - this scraper is designed for OnDemand requests")
        return []

    def results_to_data_entities(self, results: List[WebContent], 
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


# ============================================================================
# Test Functions and Usage Examples
# ============================================================================

async def test_web_search():
    """Test basic web search functionality."""
    print("=" * 60)
    print("TESTING WEB SEARCH")
    print("=" * 60)
    
    provider = FirecrawlScraperProvider()
    
    # Test search queries
    test_queries = [
        "bittensor blockchain",
        "artificial intelligence",
        "machine learning tutorials"
    ]
    
    for query in test_queries:
        print(f"\n🔍 Testing search: '{query}'")
        try:
            scraper = provider.get_scraper()
            results = await scraper.search_and_crawl(query, limit=3)
            
            print(f"   Results: {len(results)} web contents found")
            for i, result in enumerate(results):
                print(f"   [{i+1}] {result.title[:50]}...")
                print(f"       URL: {result.url}")
                print(f"       Content: {len(result.content)} chars")
                
        except Exception as e:
            print(f"   ❌ Search failed: {e}")


async def test_url_crawling():
    """Test direct URL crawling functionality."""
    print("\n" + "=" * 60)
    print("TESTING URL CRAWLING")
    print("=" * 60)
    
    scraper = FirecrawlWebScraper()
    
    # Test URLs
    test_urls = [
        "https://docs.firecrawl.dev/introduction",
        "https://github.com/anthropics/claude-code",
        "https://bittensor.com/"
    ]
    
    for url in test_urls:
        print(f"\n🌐 Testing URL: {url}")
        try:
            results = await scraper.crawl_urls([url], formats=["markdown"])
            
            if results:
                result = results[0]
                print(f"   ✅ Success: {result.title}")
                print(f"   Content: {len(result.content)} chars")
                print(f"   Author: {result.author or 'Unknown'}")
                print(f"   Language: {result.language or 'Unknown'}")
            else:
                print("   ❌ No content extracted")
                
        except Exception as e:
            print(f"   ❌ Crawling failed: {e}")


async def test_on_demand_integration():
    """Test OnDemand request handling."""
    print("\n" + "=" * 60)
    print("TESTING ON-DEMAND INTEGRATION")
    print("=" * 60)
    
    from common.protocol import OnDemandRequest
    from common.data import DataSource
    
    provider = FirecrawlScraperProvider()
    
    # Test 1: Web search query
    print("\n1. Testing web search query...")
    request1 = OnDemandRequest(
        source=DataSource.WEB_SEARCH,
        web_search_query="bittensor subnet",
        limit=3,
        formats=["markdown"]
    )
    
    try:
        entities1 = await provider.handle_web_search_request(request1)
        print(f"   Result: {len(entities1)} entities from web search")
        if entities1:
            content = WebContent.from_data_entity(entities1[0])
            print(f"   Sample: {content.title[:50]}...")
    except Exception as e:
        print(f"   ❌ Failed: {e}")
    
    # Test 2: Direct URLs
    print("\n2. Testing direct URL crawling...")
    request2 = OnDemandRequest(
        source=DataSource.WEB_SEARCH,
        urls=["https://docs.firecrawl.dev/introduction"],
        limit=5,
        formats=["markdown", "html"]
    )
    
    try:
        entities2 = await provider.handle_web_search_request(request2)
        print(f"   Result: {len(entities2)} entities from URL crawling")
        if entities2:
            content = WebContent.from_data_entity(entities2[0])
            print(f"   Sample: {content.title}")
    except Exception as e:
        print(f"   ❌ Failed: {e}")


async def test_validation():
    """Test validation functionality with sample entities."""
    print("\n" + "=" * 60)
    print("TESTING VALIDATION")
    print("=" * 60)
    
    scraper = FirecrawlWebScraper()
    
    # Create test entities (mix of valid and invalid)
    test_entities = [
        # Valid entity
        DataEntity(
            uri="https://docs.firecrawl.dev/introduction",
            datetime=dt.datetime(2024, 12, 1, 12, 0, 0, tzinfo=dt.timezone.utc),
            source=DataSource.WEB_SEARCH,
            label=DataLabel(value="web_search_firecrawl"),
            content=b'{"url": "https://docs.firecrawl.dev/introduction", "title": "Firecrawl Documentation", "content": "Firecrawl is a platform that makes it easy to crawl and convert any website into LLM-ready markdown or structured data. We crawl all accessible subpages and give you clean data for each.", "description": "Introduction to Firecrawl", "scraped_at": "2024-12-01T12:00:00+00:00"}',
            content_size_bytes=300,
        ),
        # Invalid entity (wrong source)
        DataEntity(
            uri="https://example.com/invalid",
            datetime=dt.datetime(2024, 12, 1, 12, 0, 0, tzinfo=dt.timezone.utc),
            source=DataSource.X,  # Wrong source
            label=DataLabel(value="invalid"),
            content=b'{"url": "https://example.com/invalid", "title": "Invalid", "content": "test"}',
            content_size_bytes=50,
        ),
        # Invalid entity (malformed content)
        DataEntity(
            uri="https://example.com/malformed",
            datetime=dt.datetime(2024, 12, 1, 12, 0, 0, tzinfo=dt.timezone.utc),
            source=DataSource.WEB_SEARCH,
            label=DataLabel(value="malformed"),
            content=b'invalid json content',
            content_size_bytes=20,
        ),
    ]
    
    print(f"\n🔍 Validating {len(test_entities)} entities...")
    
    try:
        validation_results = await scraper.validate(test_entities)
        
        for i, result in enumerate(validation_results):
            entity = test_entities[i] if i < len(test_entities) else None
            status = "✅ VALID" if result.is_valid else "❌ INVALID"
            print(f"   Entity {i+1}: {status}")
            print(f"      Reason: {result.reason}")
            if entity:
                print(f"      URL: {entity.uri}")
            print(f"      Bytes validated: {result.content_size_bytes_validated}")
            
    except Exception as e:
        print(f"   ❌ Validation failed: {e}")


async def test_content_model():
    """Test WebContent model functionality."""
    print("\n" + "=" * 60)
    print("TESTING WEBCONTENT MODEL")
    print("=" * 60)
    
    # Create sample WebContent
    web_content = WebContent(
        url="https://example.com/test",
        title="Test Article",
        content="This is a test article about web scraping with Firecrawl.",
        timestamp=dt.datetime.now(dt.timezone.utc),
        description="A test article",
        keywords=["test", "web scraping", "firecrawl"],
        author="Test Author",
        language="en",
        search_query="test article",
        search_rank=1
    )
    
    print("\n1. Testing WebContent creation...")
    print(f"   Title: {web_content.title}")
    print(f"   URL: {web_content.url}")
    print(f"   Content length: {len(web_content.content)} chars")
    print(f"   Keywords: {web_content.keywords}")
    
    print("\n2. Testing DataEntity conversion...")
    try:
        data_entity = web_content.to_data_entity()
        print(f"   ✅ Converted to DataEntity")
        print(f"   URI: {data_entity.uri}")
        print(f"   Source: {data_entity.source}")
        print(f"   Content size: {data_entity.content_size_bytes} bytes")
        
        # Test round-trip conversion
        print("\n3. Testing round-trip conversion...")
        restored_content = WebContent.from_data_entity(data_entity)
        print(f"   ✅ Restored from DataEntity")
        print(f"   Title match: {web_content.title == restored_content.title}")
        print(f"   URL match: {web_content.url == restored_content.url}")
        print(f"   Content match: {web_content.content == restored_content.content}")
        
    except Exception as e:
        print(f"   ❌ Conversion failed: {e}")


async def test_utils_functions():
    """Test utility functions."""
    print("\n" + "=" * 60)
    print("TESTING UTILITY FUNCTIONS")
    print("=" * 60)
    
    from scraping.web import utils
    
    # Test URL validation
    print("\n1. Testing URL validation...")
    test_urls = [
        "https://example.com",
        "http://test.org/path",
        "invalid-url",
        "ftp://example.com",
        ""
    ]
    
    for url in test_urls:
        is_valid = utils.is_valid_web_url(url)
        status = "✅" if is_valid else "❌"
        print(f"   {status} {url or '(empty)'}")
    
    # Test text similarity
    print("\n2. Testing text similarity...")
    text1 = "This is a test article about machine learning"
    text2 = "This is an article about machine learning and AI"
    text3 = "Completely different content about cooking recipes"
    
    sim1 = utils.calculate_text_similarity(text1, text2)
    sim2 = utils.calculate_text_similarity(text1, text3)
    
    print(f"   Similar texts: {sim1:.2f} similarity")
    print(f"   Different texts: {sim2:.2f} similarity")
    
    # Test random sampling
    print("\n3. Testing random sampling...")
    sample_entities = [DataEntity(uri=f"https://example.com/{i}", datetime=dt.datetime.now(dt.timezone.utc), source=DataSource.WEB_SEARCH, label=DataLabel(value="test"), content=b"test", content_size_bytes=4) for i in range(10)]
    
    sampled = utils.validate_random_url_subset(sample_entities, sample_ratio=0.3)
    print(f"   Sampled {len(sampled)} out of {len(sample_entities)} entities (30% ratio)")


if __name__ == "__main__":
    """Run all test functions."""
    import asyncio
    import datetime as dt
    from common.data import DataEntity, DataSource, DataLabel
    
    bt.logging.set_trace(True)
    
    print("🚀 Starting Firecrawl Web Scraper Tests...")
    print("Note: These tests require a valid Firecrawl API key in environment")
    
    # Run all tests
    asyncio.run(test_content_model())
    asyncio.run(test_utils_functions())
    
    # These require Firecrawl API key
    try:
        asyncio.run(test_web_search())
        asyncio.run(test_url_crawling())
        asyncio.run(test_on_demand_integration())
        asyncio.run(test_validation())
    except Exception as e:
        print(f"\n⚠️  API-dependent tests failed (check Firecrawl API key): {e}")
    
    print("\n✅ All tests completed!")