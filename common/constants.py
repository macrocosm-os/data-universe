from . import utils

# Collection of constants for use throughout the codebase.

# How big any one data entity bucket can be to limit size over the wire.
DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES = utils.mb_to_bytes(128)
# How many data entity buckets any one miner index can have to limit necessary storage on the validators.
DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX = 3000000
# How old a data entity bucket can be before the validators do not assign any value for them.
DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS = 30

# The current protocol version.
PROTOCOL_VERSION = 1
