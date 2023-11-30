"""General utility functions."""

_KB = 1024
_MB = 1024 * _KB


def mb_to_bytes(mb: int) -> int:
    """Returns the total number of bytes."""
    return mb * _MB


def seconds_to_hours(seconds: int) -> int:
    """Returns the total number of hours, rounded down."""
    return seconds // 3600
