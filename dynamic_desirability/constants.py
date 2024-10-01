NETWORK: str = 'finney'   # FOR TESTING
NETUID: int = 13        # FOR TESTING

# The link to the github repo where preferences JSONs are uploaded.
REPO_URL: str = 'https://github.com/macrocosm-os/dynamic-desirability-test.git'
BRANCH_NAME: str = 'main'

# Total weight of all validators. Subnet (default) voting weight = 1-TOTAL_VALI_WEIGHT. 
TOTAL_VALI_WEIGHT: float = 0.7

# Paths of subnet preferences (default) and overall subnet + validator preferences.
DEFAULT_JSON_PATH: str = 'default.json'
AGGREGATE_JSON_PATH: str = 'total.json'

# Data source weights.
REDDIT_SOURCE_WEIGHT: float = 0.6
X_SOURCE_WEIGHT: float = 0.4