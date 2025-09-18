"""
Firecrawl-based web search and content extraction scraper for Data Universe subnet.
"""

import asyncio
import json
from typing import List, Optional
from datetime import datetime, timezone
import bittensor as bt
from firecrawl import Firecrawl
from common.data import DataEntity, DataSource, DataLabel, TimeBucket
from common.protocol import OnDemandRequest
from scraping.scraper import Scraper, ValidationResult
from scraping.provider import ScraperProvider
from scraping.web.model import WebContent
from scraping.web import utils


class FirecrawlWebScraper(Scraper):
    """Clean Firecrawl-based web scraper following """

    def __init__(self, api_key: str = None):
        """Initialize Firecrawl web scraper"""
        try:
            self.firecrawl = Firecrawl(api_key='fc-6e9855c37813450aac5e4f463f9c7268')
            bt.logging.info("Firecrawl web scraper initialized")
        except Exception as e:
            bt.logging.error(f"Failed to initialize Firecrawl: {e}")
            raise

    async def asearch(self, query: str, limit: int = 10) -> List[WebContent]:
        """Search using Firecrawl API"""
        try:
            bt.logging.debug(f"Firecrawl search: {query}")

            loop = asyncio.get_event_loop()
            search_result = await loop.run_in_executor(
                None,
                lambda: self.firecrawl.search(query, limit=limit)
            )

            if not search_result or not hasattr(search_result, 'web'):
                return []

            results = []
            for item in search_result.web[:limit]:
                url = getattr(item, 'url', '')
                title = getattr(item, 'title', '')
                description = getattr(item, 'description', '')

                if not url or not title:
                    continue

                content = description or f"Search result: {title}"
                web_content = WebContent(
                    url=url,
                    title=title,
                    content=content,
                    timestamp=datetime.now(timezone.utc),
                    description=description,
                    keywords=[],
                    author=None,
                    language=None,
                    content_type="search_result",
                    word_count=len(content.split()),
                    images=[],
                    links=[],
                    search_query=query,
                    search_rank=len(results) + 1
                )
                results.append(web_content)

            return results

        except Exception as e:
            bt.logging.error(f"Firecrawl search failed: {e}")
            return []

    async def acrawl(self, url: str) -> Optional[WebContent]:
        """Crawl single URL using Firecrawl"""
        try:
            loop = asyncio.get_event_loop()
            scrape_result = await loop.run_in_executor(
                None,
                lambda: self.firecrawl.scrape(url, formats=["markdown"])
            )

            if not scrape_result:
                return None

            content = getattr(scrape_result, 'markdown', '') or getattr(scrape_result, 'content', '')
            metadata = getattr(scrape_result, 'metadata', {})

            if hasattr(metadata, '__dict__'):
                metadata = metadata.__dict__
            elif not isinstance(metadata, dict):
                metadata = {}

            title = metadata.get('title', '') or getattr(scrape_result, 'title', '')

            return WebContent(
                url=url,
                title=title,
                content=content,
                timestamp=datetime.now(timezone.utc),
                description=metadata.get('description', ''),
                keywords=metadata.get('keywords', []) if isinstance(metadata.get('keywords'), list) else [],
                author=metadata.get('author', ''),
                language=metadata.get('language', ''),
                content_type="scraped_content",
                word_count=len(content.split()) if content else 0,
                images=metadata.get('images', []) if isinstance(metadata.get('images'), list) else [],
                links=metadata.get('links', []) if isinstance(metadata.get('links'), list) else []
            )

        except Exception as e:
            bt.logging.error(f"Firecrawl crawl failed for {url}: {e}")
            return None

    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        """Validate web search DataEntities"""
        if not entities:
            return []

        # Random sampling
        entities_to_validate = utils.validate_random_url_subset(entities, sample_ratio=0.3)
        bt.logging.info(f"Validating {len(entities_to_validate)} out of {len(entities)} web entities")

        results = []

        for entity in entities_to_validate:
            try:
                if entity.source != DataSource.WEB_SEARCH:
                    results.append(ValidationResult(
                        is_valid=False,
                        reason="Entity is not from WEB_SEARCH source",
                        content_size_bytes_validated=entity.content_size_bytes
                    ))
                    continue

                try:
                    miner_content = WebContent.from_data_entity(entity)
                except Exception as e:
                    results.append(ValidationResult(
                        is_valid=False,
                        reason=f"Failed to parse WebContent: {str(e)}",
                        content_size_bytes_validated=entity.content_size_bytes
                    ))
                    continue

                if not utils.is_valid_web_url(miner_content.url):
                    results.append(ValidationResult(
                        is_valid=False,
                        reason="Invalid URL format",
                        content_size_bytes_validated=entity.content_size_bytes
                    ))
                    continue

                # Re-scrape for validation
                validator_content = await self.acrawl(miner_content.url)

                if validator_content is None:
                    results.append(ValidationResult(
                        is_valid=False,
                        reason="Failed to re-scrape URL for validation",
                        content_size_bytes_validated=entity.content_size_bytes
                    ))
                    continue

                validation_result = utils.validate_web_content_with_rescrape(
                    miner_content=miner_content,
                    validator_scraped_content=validator_content,
                    entity=entity
                )

                results.append(validation_result)

            except Exception as e:
                bt.logging.error(f"Error validating entity: {e}")
                results.append(ValidationResult(
                    is_valid=False,
                    reason=f"Validation exception: {str(e)}",
                    content_size_bytes_validated=entity.content_size_bytes if entity else 0
                ))

        # Mark non-validated entities as valid
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


class FirecrawlScraperProvider(ScraperProvider):
    """Provider for Firecrawl-based web scraping"""

    def __init__(self, api_key: str = None):
        self.api_key = api_key
        self.scraper = None

    def get_scraper(self) -> FirecrawlWebScraper:
        """Get or create Firecrawl scraper instance"""
        if self.scraper is None:
            self.scraper = FirecrawlWebScraper(api_key=self.api_key)
        return self.scraper

    async def handle_web_search_request(self, request: OnDemandRequest) -> List[DataEntity]:
        """Handle web search OnDemandRequest"""
        scraper = self.get_scraper()
        time_bucket = TimeBucket.from_datetime(datetime.now(timezone.utc))
        results = []

        # Handle web search query
        if request.web_search_query:
            search_results = await scraper.asearch(
                query=request.web_search_query,
                limit=request.limit or 10
            )
            results.extend(search_results)

        # Handle specific URLs
        if request.urls:
            for url in request.urls:
                crawl_result = await scraper.acrawl(url)
                if crawl_result:
                    results.append(crawl_result)

        # Convert to DataEntity objects
        entities = []
        for result in results:
            try:
                entity = result.to_data_entity()
                entities.append(entity)
            except Exception as e:
                bt.logging.error(f"Failed to convert result to DataEntity: {e}")
                continue

        bt.logging.info(f"Firecrawl web search completed: {len(entities)} entities created")
        return entities


# Simple test functions
async def test_search():
    """Test search functionality"""
    scraper = FirecrawlWebScraper()
    results = await scraper.asearch("python programming", limit=3)
    print(f"Search results: {len(results)}")
    for i, result in enumerate(results):
        print(f"  {i+1}. {result.title} - {result.url}")

async def test_crawl():
    """Test crawl functionality"""
    scraper = FirecrawlWebScraper()
    result = await scraper.acrawl("https://docs.firecrawl.dev/introduction")
    if result:
        print(f"Crawled: {result.title}")
        print(f"Content length: {len(result.content)} chars")
    else:
        print("Crawl failed")

if __name__ == "__main__":
    import asyncio
    bt.logging.set_trace(True)

    print("🚀 Testing Firecrawl Web Scraper (Clean Version)")
    asyncio.run(test_search())
    asyncio.run(test_crawl())
    print("✅ Tests completed")