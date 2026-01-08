import datetime as dt
from . import utils

# Collection of constants for use throughout the codebase.

# How big any one data entity bucket can be to limit size over the wire.
DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES = utils.mb_to_bytes(128)

# How many data entity buckets any one miner index can have to limit necessary storage on the validators.
DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX = 200_000
DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX_PROTOCOL_3 = 250_000
DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX_PROTOCOL_4 = 350_000

# How big the collection of contents can be to limit size over the wire.
BULK_CONTENTS_SIZE_LIMIT_BYTES = utils.mb_to_bytes(128)
BULK_CONTENTS_COUNT_LIMIT = 200_000

# How many different buckets can be requests at once.
BULK_BUCKETS_COUNT_LIMIT = 100

# How old a data entity bucket can be before the validators do not assign any value for them.
DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS = 30

# The maximum number of characters a label can have.
MAX_LABEL_LENGTH = 140

# The current protocol version (int)
PROTOCOL_VERSION = 4

# Min evaluation period that must pass before a validator re-evaluates a miner.
MIN_EVALUATION_PERIOD = dt.timedelta(minutes=60)

# Miner compressed index cache freshness.
MINER_CACHE_FRESHNESS = dt.timedelta(minutes=20)

# Date after which only x.com URLs are accepted
NO_TWITTER_URLS_DATE = dt.datetime(2024, 12, 28, tzinfo=dt.timezone.utc)  # December 28, 2024 UTC

# Date after which media content is required for Reddit posts that contain media
REDDIT_MEDIA_REQUIRED_DATE = dt.datetime(2025, 8, 7, tzinfo=dt.timezone.utc)  # August 7, 2025 UTC (one month from now)

# Date after which backwards compatibility for nested X content format will be removed
X_ENHANCED_FORMAT_COMPATIBILITY_EXPIRATION_DATE = dt.datetime(2025, 9, 30, tzinfo=dt.timezone.utc) # September 30, 2025 UTC

# Date after which filename format validation is enforced (data_{YYYYMMDD_HHMMSS}_{count}_{16hex}.parquet)
FILENAME_FORMAT_REQUIRED_DATE = dt.datetime(2026, 1, 18, tzinfo=dt.timezone.utc) # January 18, 2026 UTC

EVALUATION_ON_STARTUP = 15

# Emission Control / Burn Configuration
EMISSION_CONTROL_PERCENTAGE = 0.60  # 60% of emissions redirected to subnet owner (burn)
