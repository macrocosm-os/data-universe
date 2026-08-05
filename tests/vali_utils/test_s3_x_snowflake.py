import datetime as dt

import pandas as pd

from vali_utils.s3_utils import _is_x_snowflake_timestamp_consistent


def test_modern_x_snowflake_matches_claimed_minute():
    assert _is_x_snowflake_timestamp_consistent(
        "1748585332935622672",
        dt.datetime(2024, 1, 20, 5, 56, tzinfo=dt.timezone.utc),
    )


def test_synthetic_x_id_does_not_match_modern_timestamp():
    assert not _is_x_snowflake_timestamp_consistent(
        "1090517116589",
        dt.datetime(2026, 4, 10, 12, 15, tzinfo=dt.timezone.utc),
    )


def test_non_numeric_x_id_is_invalid_for_modern_timestamp():
    assert not _is_x_snowflake_timestamp_consistent(
        "not-a-tweet-id",
        dt.datetime(2026, 4, 10, 12, 15, tzinfo=dt.timezone.utc),
    )


def test_pre_snowflake_tweet_is_not_rejected():
    assert _is_x_snowflake_timestamp_consistent(
        "12345",
        dt.datetime(2009, 4, 10, 12, 15, tzinfo=dt.timezone.utc),
    )


def test_nat_datetime_is_rejected_not_raised():
    assert not _is_x_snowflake_timestamp_consistent("1748585332935622672", pd.NaT)


def test_none_datetime_is_rejected():
    assert not _is_x_snowflake_timestamp_consistent("1748585332935622672", None)


def test_null_tweet_id_with_modern_timestamp_is_rejected():
    ts = dt.datetime(2026, 4, 10, 12, 15, tzinfo=dt.timezone.utc)
    assert not _is_x_snowflake_timestamp_consistent(None, ts)
    assert not _is_x_snowflake_timestamp_consistent(float("nan"), ts)
