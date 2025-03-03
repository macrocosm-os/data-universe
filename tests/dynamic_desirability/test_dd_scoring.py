import os
import unittest
import datetime as dt
import bittensor as bt
from common import constants, utils
from dynamic_desirability import desirability_retrieval
from common.data import DataSource, TimeBucket
from common.data_v2 import ScorableDataEntityBucket
from rewards.data_value_calculator import DataValueCalculator
from dynamic_desirability.constants import (
                                            DEFAULT_JSON_PATH,
                                            AGGREGATE_JSON_PATH,
                                            TOTAL_VALI_WEIGHT
                                            )



class TestDDScoring(unittest.TestCase):
    def setUp(self):
        try:
            my_wallet = bt.wallet(name = "test_wallet", hotkey="test_hotkey")
            subtensor = bt.subtensor(network="finney")
            metagraph = subtensor.metagraph(netuid=13)

            bt.logging.info("\nGetting validator weights from the metagraph...\n")
            validator_data = utils.get_validator_data(metagraph=metagraph, vpermit_rao_limit=10_000)

            bt.logging.info("\nRetrieving latest validator commit hashes from the chain (This takes ~90 secs)...\n")

            for hotkey in validator_data.keys():
                uid = subtensor.get_uid_for_hotkey_on_subnet(hotkey_ss58=hotkey, netuid=13)
                validator_data[hotkey]['github_hash'] =  subtensor.get_commitment(netuid=13, uid=uid)
                if validator_data[hotkey]['github_hash']:
                    validator_data[hotkey]['json'] = desirability_retrieval.get_json(commit_sha=validator_data[hotkey]['github_hash'],
                                                            filename=f"{hotkey}.json")

            bt.logging.info("\nCalculating total weights...\n")

            # script_dir = os.path.dirname(os.path.abspath(__file__))
            default_path = f"dynamic_desirability/{DEFAULT_JSON_PATH}"
            desirability_retrieval.calculate_total_weights(validator_data=validator_data, default_json_path=default_path,
                                    total_vali_weight=TOTAL_VALI_WEIGHT)

            self.model = desirability_retrieval.to_lookup(json_file=f"dynamic_desirability/{AGGREGATE_JSON_PATH}")

        except Exception as e:
            bt.logging.error(f"Could not retrieve dynamic preferences. Using default.json to build lookup: {str(e)}")
            # script_dir = os.path.dirname(os.path.abspath(__file__))
            self.model = desirability_retrieval.to_lookup(json_file=f"dynamic_desirability/{DEFAULT_JSON_PATH}")
        
        self.value_calculator = DataValueCalculator(model=self.model)


    def get_score_for_label(self, label: str, score: float):
        
        now = dt.datetime(2023, 12, 12, 12, 30, 0, tzinfo=dt.timezone.utc)
        current_time_bucket = TimeBucket.from_datetime(now)

        # Verify the score past the max age is 0 for regular labels.
        bucket = ScorableDataEntityBucket(
            time_bucket_id=utils.time_bucket_id_from_datetime(
                now
                - dt.timedelta(
                    hours=constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS * 24 + 1
                )
            ),
            source=DataSource.REDDIT,
            label=label,
            size_bytes=200,
            # scorable_bytes is different from size_bytes to ensure the score is based on scorable_bytes.
            scorable_bytes=100,
        )

        self.assertAlmostEqual(
            self.value_calculator.get_score_for_data_entity_bucket(
                bucket, current_time_bucket
            ),
            score,
            places=5,
        )
    
    def test_non_dd_label_score(self):
        self.get_score_for_label(label="r/testlabel", score=0)

    def test_dd_label_score(self):
        self.get_score_for_label(label="r/bittensor_", score=0.6 * 0.6 * 100)

if __name__ == "__main__":
    unittest.main()
