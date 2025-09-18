"""
Web scraping module for Data Universe subnet.
Contains Firecrawl-based web search and content extraction functionality.
"""

from .firecrawl_scraper import FirecrawlWebScraper, FirecrawlScraperProvider

__all__ = [
    'FirecrawlWebScraper',
    'FirecrawlScraperProvider',
]