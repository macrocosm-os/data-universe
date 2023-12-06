from abc import ABC


import abc
import dataclasses

from common.data import DataEntity


@dataclasses.dataclass(frozen=True)
class ValidationResult:
    """Data class to contain the result of a scraping validation."""

    # For now, let's just indicate a pass/fail, but in future we may want to extend this to
    # include more information about the validation.
    is_valid: bool


class Scraper(abc.ABC):
    """An abstract base class for scrapers across all data sources.

    A scraper should be able to scrape batches of data and verify the correctness of a DataEntity by URI.
    """

    @abc.abstractmethod
    def validate(self, entity: DataEntity) -> ValidationResult:
        """_summary_

        Args:
            uri (str): _description_

        Returns:
            ValidationResult: _description_
        """
        ...
