from abc import ABC, abstractmethod
import bittensor as bt
from typing import List, Optional
import datetime as dt
from common.data import DataLabel, DataSource, DataEntity, DateRange
from scraping.scraper import ScrapeConfig, Scraper


class OnDemandScraper(Scraper, ABC):
    """Base class for on-demand data scrapers"""

    def __init__(self, source: DataSource):
        self.source = source

    async def handle_request(self,
                             keywords: List[str],
                             usernames: List[str],
                             start_date: Optional[str],
                             end_date: Optional[str],
                             limit: int) -> List[DataEntity]:
        """
        Handle an on-demand scraping request
        """
        try:
            # Convert dates to datetime if provided
            start_dt = (dt.datetime.fromisoformat(start_date)
                        if start_date else dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1))
            end_dt = (dt.datetime.fromisoformat(end_date)
                      if end_date else dt.datetime.now(dt.timezone.utc))

            # Create labels from keywords and usernames
            labels = []
            if keywords:
                labels.extend([DataLabel(value=k) for k in keywords])
            if usernames:
                labels.extend([DataLabel(value=f"@{u}") for u in usernames])

            # Create scrape config
            config = ScrapeConfig(
                entity_limit=limit,
                date_range=DateRange(
                    start=start_dt,
                    end=end_dt
                ),
                labels=labels,
            )

            # Use existing scrape method
            data = await self.scrape(config)
            return data[:limit] if limit else data

        except Exception as e:
            bt.logging.error(f"Error in on-demand scraping: {str(e)}")
            return []

    @property
    def source_type(self) -> DataSource:
        """Get the data source type"""
        return self.source