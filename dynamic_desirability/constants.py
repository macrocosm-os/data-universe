# The link to the github repo where preferences JSONs are uploaded.
REPO_URL: str = 'https://github.com/macrocosm-os/gravity.git'
BRANCH_NAME: str = 'main'
PREFERENCES_FOLDER: str = 'validator_preferences'

# Total weight of all validators. Subnet (default) voting weight = 1-TOTAL_VALI_WEIGHT. 
TOTAL_VALI_WEIGHT: float = 0.7
DEFAULT_SCALE_FACTOR: float = 0.3               # number is subject to change
AMPLICATION_FACTOR: float = 250 / TOTAL_VALI_WEIGHT * (1 - TOTAL_VALI_WEIGHT)

# Paths of subnet preferences (default) and overall subnet + validator preferences.
DEFAULT_JSON_PATH: str = 'default.json'
AGGREGATE_JSON_PATH: str = 'total.json'

VALID_SOURCES: dict[str, str] = {
    "reddit": "r/",
    "x": "#",
    "youtube": "",
}