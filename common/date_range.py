import dataclasses
import datetime as dt


@dataclasses.dataclass(frozen=True)
class DateRange:
    """Represents a specific time range from start time inclusive to end time exclusive."""

    # The start time inclusive of the time range.
    start: dt.datetime

    # The end time exclusive of the time range.
    end: dt.datetime

    def contains(self, datetime: dt.datetime) -> bool:
        """Returns True if the provided datetime is within this DateRange."""
        return self.start <= datetime < self.end
