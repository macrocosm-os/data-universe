import datetime
from . import utils

# Collection of constants for use throughout the codebase.

# How big any one data entity bucket can be to limit size over the wire.
DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES = utils.mb_to_bytes(128)

# How many data entity buckets any one miner index can have to limit necessary storage on the validators.
DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX = 200_000
DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX_PROTOCOL_3 = 250_000
DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX_PROTOCOL_4 = 350_000

# How old a data entity bucket can be before the validators do not assign any value for them.
DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS = 30

# The maximum number of characters a label can have.
MAX_LABEL_LENGTH = 32

# The current protocol version (int)
PROTOCOL_VERSION = 4

# Baseline threshold under which score increase limits are not applied.
SCORE_GROWTH_LIMIT_THRESHOLD = utils.mb_to_bytes(1000)

# Percent limit for score increase in a single evaluation.
SCORE_GROWTH_LIMIT_PERCENT = 1.05

# Min evaluation period that must pass before a validator re-evaluates a miner.
MIN_EVALUATION_PERIOD = datetime.timedelta(minutes=60)

# Datetime after which DataEntity content field has lower granularity dates.
REDUCED_CONTENT_DATETIME_GRANULARITY_THRESHOLD = datetime.datetime(
    year=2024, month=3, day=1, tzinfo=datetime.timezone.utc
)
