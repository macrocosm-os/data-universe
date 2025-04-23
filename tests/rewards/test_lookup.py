from common import constants
from common.data import DataLabel, DataSource
from rewards.data import DynamicSourceDesirability, DynamicDesirabilityLookup

#################################################################

# This list is outdated and is only used as a backup to Dynamic Desirability. 
# Please see the folder dynamic_desirability for more information on how reward 
# scale factors are constructed. 

#################################################################

LOOKUP = DynamicDesirabilityLookup(
    distribution={
        DataSource.REDDIT: DynamicSourceDesirability(
            weight=0.6,
            default_scale_factor=0.3,
            label_config={
                DataLabel(value="r/Bitcoin"): (1.0, None),
                DataLabel(value="r/BitcoinCash"): (1.0, None),
                DataLabel(value="r/Bittensor_"): (1.0, None),
                DataLabel(value="r/Btc"): (1.0, None),
                DataLabel(value="r/Cryptocurrency"): (1.0, None),
                DataLabel(value="r/Cryptomarkets"): (1.0, None),
                DataLabel(value="r/EthereumClassic"): (1.0, None),
                DataLabel(value="r/Ethtrader"): (1.0, None),
                DataLabel(value="r/Filecoin"): (1.0, None),
                DataLabel(value="r/Monero"): (1.0, None),
                DataLabel(value="r/Polkadot"): (1.0, None),
                DataLabel(value="r/Solana"): (1.0, None),
                DataLabel(value="r/WallstreetBets"): (1.0, None),
            },
        ),
        DataSource.X: DynamicSourceDesirability(
            weight=0.4,
            default_scale_factor=0.3,
            label_config={
                DataLabel(value="#bitcoin"): (1.0, None),
                DataLabel(value="#bitcoincharts"): (1.0, None),
                DataLabel(value="#bitcoiner"): (1.0, None),
                DataLabel(value="#bitcoinexchange"): (1.0, None),
                DataLabel(value="#bitcoinmining"): (1.0, None),
                DataLabel(value="#bitcoinnews"): (1.0, None),
                DataLabel(value="#bitcoinprice"): (1.0, None),
                DataLabel(value="#bitcointechnology"): (1.0, None),
                DataLabel(value="#bitcointrading"): (1.0, None),
                DataLabel(value="#bittensor"): (1.0, None),
                DataLabel(value="#btc"): (1.0, None),
                DataLabel(value="#cryptocurrency"): (1.0, None),
                DataLabel(value="#crypto"): (1.0, None),
                DataLabel(value="#defi"): (1.0, None),
                DataLabel(value="#decentralizedfinance"): (1.0, None),
                DataLabel(value="#tao"): (1.0, None),
            },
        ),
    },
    max_age_in_hours=constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS * 24,
)