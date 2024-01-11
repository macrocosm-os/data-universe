from common import constants
from common.data import DataLabel, DataSource
from rewards.data import DataSourceDesirability, DataDesirabilityLookup

#################################################################

# We welcome feedback on this list.
# Please file an Issue on the github with your suggestions.

#################################################################

LOOKUP = DataDesirabilityLookup(
    distribution={
        DataSource.REDDIT: DataSourceDesirability(
            weight=0.25,  # Weight Reddit data less heavily due to the ease of mining it using a personal reddit account.
            default_scale_factor=0.5,
            label_scale_factors={
                DataLabel(value="Bitcoin"): 1.0,
                DataLabel(value="BitcoinCash"): 1.0,
                DataLabel(value="Bittensor_"): 1.0,
                DataLabel(value="Btc"): 1.0,
                DataLabel(value="Cryptocurrency"): 1.0,
                DataLabel(value="Cryptomarkets"): 1.0,
                DataLabel(value="EthereumClassic"): 1.0,
                DataLabel(value="Ethtrader"): 1.0,
                DataLabel(value="Filecoin"): 1.0,
                DataLabel(value="Monero"): 1.0,
                DataLabel(value="Polkadot"): 1.0,
                DataLabel(value="Solana"): 1.0,
                DataLabel(value="WallstreetBets"): 1.0,
            },
        ),
        DataSource.X: DataSourceDesirability(
            weight=0.75,
            default_scale_factor=0.5,
            label_scale_factors={
                DataLabel(value="#bitcoin"): 1.0,
                DataLabel(value="#bitcoincharts"): 1.0,
                DataLabel(value="#bitcoiner"): 1.0,
                DataLabel(value="#bitcoinexchange"): 1.0,
                DataLabel(value="#bitcoinmining"): 1.0,
                DataLabel(value="#bitcoinnews"): 1.0,
                DataLabel(value="#bitcoinprice"): 1.0,
                DataLabel(value="#bitcointechnology"): 1.0,
                DataLabel(value="#bitcointrading"): 1.0,
                DataLabel(value="#bittensor"): 1.0,
                DataLabel(value="#btc"): 1.0,
                DataLabel(value="#cryptocurrency"): 1.0,
                DataLabel(value="#crypto"): 1.0,
                DataLabel(value="#defi"): 1.0,
                DataLabel(value="#decentralizedfinance"): 1.0,
                DataLabel(value="#tao"): 1.0,
            },
        ),
    },
    max_age_in_hours=constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS * 24,
)
