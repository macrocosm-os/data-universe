"""Regression tests for media-only tweet handling in DuckDBSampledValidator.

Empty tweet text is legitimate for media-only tweets, so ``_create_twitter_entity``
must not reject such rows. Rows missing a username or URL must still be rejected
so fabrication detection is unchanged.
"""
from common.data import DataEntity
from vali_utils.s3_utils import DuckDBSampledValidator


def _make_validator():
    """Create a validator instance without running the network-bound __init__."""
    return DuckDBSampledValidator.__new__(DuckDBSampledValidator)


def test_media_only_tweet_returns_entity():
    """A media-only tweet (empty text, username + url present) must be accepted."""
    validator = _make_validator()
    row = {
        "username": "bob",
        "text": "",
        "url": "https://x.com/bob/status/1",
        "tweet_id": "1",
    }

    entity = validator._create_twitter_entity(row)

    assert isinstance(entity, DataEntity)
    assert entity.uri == "https://x.com/bob/status/1"


def test_missing_username_returns_none():
    """A row without a username must still be rejected."""
    validator = _make_validator()
    row = {"username": "", "text": "hello", "url": "https://x.com/bob/status/2"}

    assert validator._create_twitter_entity(row) is None


def test_missing_url_returns_none():
    """A row without a URL must still be rejected."""
    validator = _make_validator()
    row = {"username": "bob", "text": "hello", "url": ""}

    assert validator._create_twitter_entity(row) is None
