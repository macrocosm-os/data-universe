from common import constants
from common.data import DataLabel, DataSource
from rewards.data import DataSourceDesirability, DataDesirabilityLookup

#################################################################

# This list is outdated and is only used as a backup to Dynamic Desirability. 
# Please see the folder dynamic_desirability for more information on how reward 
# scale factors are constructed. 

#################################################################

LOOKUP = DataDesirabilityLookup(
    distribution={
        DataSource.REDDIT: DataSourceDesirability(
            weight=0.6,
            default_scale_factor=0.5,
            label_scale_factors={
                DataLabel(value="r/Bitcoin"): 1.0,
                DataLabel(value="r/BitcoinCash"): 1.0,
                DataLabel(value="r/Bittensor_"): 1.0,
                DataLabel(value="r/Btc"): 1.0,
                DataLabel(value="r/Cryptocurrency"): 1.0,
                DataLabel(value="r/Cryptomarkets"): 1.0,
                DataLabel(value="r/EthereumClassic"): 1.0,
                DataLabel(value="r/Ethtrader"): 1.0,
                DataLabel(value="r/Filecoin"): 1.0,
                DataLabel(value="r/Monero"): 1.0,
                DataLabel(value="r/Polkadot"): 1.0,
                DataLabel(value="r/Solana"): 1.0,
                DataLabel(value="r/WallstreetBets"): 1.0,
            },
        ),
        DataSource.X: DataSourceDesirability(
            weight=0.4,
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
