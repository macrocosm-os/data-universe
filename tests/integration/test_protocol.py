from typing import Callable, Tuple, List
import unittest
import bittensor as bt
import datetime as dt
from common import old_protocol
from common.data import (
    CompressedEntityBucket,
    CompressedMinerIndex,
    DataEntity,
    DataEntityBucket,
    DataEntityBucketId,
    DataLabel,
    DataSource,
    TimeBucket,
    HuggingFaceMetadata,
)

from common.protocol import GetDataEntityBucket, GetMinerIndex, GetHuggingFaceMetadata
from storage.miner.miner_storage import MinerStorage
from storage.miner.sqlite_miner_storage import SqliteMinerStorage
from storage.validator.sqlite_memory_validator_storage import (
    SqliteMemoryValidatorStorage,
)
from vali_utils import utils as vali_utils
from tests import utils as test_utils


class FakeMiner:
    """A simple implementation of a miner that defines the protocol forward functions."""

    def __init__(
            self,
            index: GetMinerIndex = None,
            entities: GetDataEntityBucket = None,
            storage: MinerStorage = None,
    ):
        self.index = index
        self.entities = entities
        self.storage = storage

    def get_miner_index(self, request: GetMinerIndex) -> GetMinerIndex:
        # If we have storage, use it. Otherwise fallback to the provided index
        if self.storage:
            request.compressed_index_serialized = (
                self.storage.get_compressed_index().json()
            )
            return request
        return self.index

    def get_data_bucket(
            self, request: old_protocol.GetDataEntityBucket
    ) -> old_protocol.GetDataEntityBucket:
        return self.entities

    def get_huggingface_metadata(self, request: GetHuggingFaceMetadata) -> GetHuggingFaceMetadata:
        if self.storage:
            request.metadata = self.storage.get_hf_metadata()
        return request


class IntegrationTestProtocol(unittest.TestCase):
    def _insert_and_get_index(self, storage: MinerStorage) -> CompressedMinerIndex:
        now = dt.datetime.now()
        # Create an entity for bucket 1.
        bucket1_datetime = now
        bucket2_datetime = now + dt.timedelta(hours=1)

        # Store the entities.
        storage.store_data_entities(
            [
                DataEntity(
                    uri="test_entity_1",
                    datetime=bucket1_datetime,
                    source=DataSource.REDDIT,
                    label=DataLabel(value="label_1"),
                    content=bytes(10),
                    content_size_bytes=10,
                ),
                DataEntity(
                    uri="test_entity_2",
                    datetime=bucket2_datetime,
                    source=DataSource.X,
                    label=DataLabel(value="label_2"),
                    content=bytes(20),
                    content_size_bytes=20,
                ),
            ]
        )

        return CompressedMinerIndex(
            sources={
                DataSource.REDDIT: [
                    CompressedEntityBucket(
                        label="label_1",
                        time_bucket_ids=[TimeBucket.from_datetime(bucket1_datetime).id],
                        sizes_bytes=[10],
                    )
                ],
                DataSource.X: [
                    CompressedEntityBucket(
                        label="label_2",
                        time_bucket_ids=[TimeBucket.from_datetime(bucket2_datetime).id],
                        sizes_bytes=[20],
                    )
                ],
            }
        )

    def _insert_test_hf_metadata(self, storage: MinerStorage) -> List[HuggingFaceMetadata]:
        now = dt.datetime.utcnow()
        test_metadata = [
            HuggingFaceMetadata(
                repo_name="test_repo_1",
                source=DataSource.REDDIT,
                updated_at=now
            ),
            HuggingFaceMetadata(
                repo_name="test_repo_2",
                source=DataSource.X,
                updated_at=now + dt.timedelta(hours=1)
            ),
        ]
        storage.store_hf_dataset_info(test_metadata)
        return test_metadata

    def setUp(self):
        # Enable logging
        bt.logging(
            # debug=True,
            trace=True
        )

        self.ip = bt.utils.networking.get_external_ip()

        self.wallet = bt.wallet(name="unit_test2", hotkey="unit_test2")
        self.wallet.create_if_non_existent(
            coldkey_use_password=False, hotkey_use_password=False
        )

        # Make a test database for the test to operate against.
        self.vali_storage = SqliteMemoryValidatorStorage()
        self.miner_storage = SqliteMinerStorage()

    def _test_round_trip(self, forward_fn: Callable, request: bt.Epistula) -> bt.Epistula:
        """Base test for verifying a protocol message between dendrite and axon."""
        port = 1234
        axon = bt.axon(
            wallet=self.wallet,
            ip="0.0.0.0",
            port=port,
            external_port=port,
        )

        try:
            axon.attach(forward_fn=forward_fn)
            axon.start()

            dendrite = bt.dendrite(wallet=self.wallet)

            response = dendrite.query(
                axons=bt.AxonInfo(
                    version=1,
                    ip=self.ip,
                    port=port,
                    ip_type=1,
                    hotkey=self.wallet.hotkey.ss58_address,
                    coldkey=self.wallet.coldkey.ss58_address,
                ),
                synapse=request,
                timeout=12000,
            )

            self.assertTrue(response.is_success)

            return response
        finally:
            axon.stop()

    def _create_test_index(self) -> CompressedMinerIndex:
        """Returns a CompressedMinerIndex for testing."""
        return CompressedMinerIndex(
            sources={
                DataSource.REDDIT.value: [
                    CompressedEntityBucket(
                        label="r/bittensor_",
                        time_bucket_ids=[1, 2, 3],
                        sizes_bytes=[100, 200, 300],
                    )
                ],
                DataSource.X.value: [
                    CompressedEntityBucket(
                        time_bucket_ids=[3, 4],
                        sizes_bytes=[123, 234],
                    ),
                    CompressedEntityBucket(
                        label="#bittensor",
                        time_bucket_ids=[5, 4],
                        sizes_bytes=[321, 99],
                    ),
                ],
            }
        )

    def test_get_miner_index(self):
        """Tests a round trip using the new compressed miner format."""
        compressed_index = self._create_test_index()

        expected_response = GetMinerIndex(
            compressed_index_serialized=compressed_index.json(),
        )

        # Send a request to the fake miner to get the miner index.
        request = GetMinerIndex()
        miner = FakeMiner(expected_response, None)
        response = self._test_round_trip(miner.get_miner_index, request)

        # We can't compare the responses directly because the bittensor header info will differ.
        self.assertEqual(
            expected_response.compressed_index_serialized,
            response.compressed_index_serialized,
        )

        # Now that we have the response, write it to the vali DB.
        got_compressed_index = vali_utils.get_miner_index_from_response(response)
        self.assertEqual(got_compressed_index, compressed_index)

        # Store in the vali DB.
        self.vali_storage.upsert_compressed_miner_index(
            got_compressed_index, self.wallet.hotkey.ss58_address, 1
        )

        # Finally, read it out and verify it's as expected.
        scorable_index = self.vali_storage.read_miner_index(
            self.wallet.hotkey.ss58_address
        )
        expected_scorable_index = (
            test_utils.convert_compressed_index_to_scorable_miner_index(
                compressed_index, scorable_index.last_updated
            )
        )
        equal, reason = test_utils.are_scorable_indexes_equal(
            expected_scorable_index, scorable_index
        )
        if not equal:
            self.fail(reason)
        self.assertTrue(
            scorable_index.last_updated - dt.datetime.utcnow()
            < dt.timedelta(seconds=30)
        )

    def test_get_compressed_miner_index(self):
        """Tests a round trip using the new compressed miner format."""
        expected_index = self._insert_and_get_index(self.miner_storage)

        # Send a request to the fake miner to get the miner index.
        request = GetMinerIndex()
        miner = FakeMiner(storage=self.miner_storage)
        response = self._test_round_trip(miner.get_miner_index, request)

        # Now that we have the response, write it to the vali DB.
        got_compressed_index = vali_utils.get_miner_index_from_response(response)
        self.assertTrue(
            test_utils.are_compressed_indexes_equal(
                got_compressed_index, expected_index
            )
        )

        # Store in the vali DB.
        self.vali_storage.upsert_compressed_miner_index(
            got_compressed_index, self.wallet.hotkey.ss58_address, 1
        )

        # Finally, read it out and verify it's as expected.
        scorable_index = self.vali_storage.read_miner_index(
            self.wallet.hotkey.ss58_address
        )
        expected_scorable_index = (
            test_utils.convert_compressed_index_to_scorable_miner_index(
                expected_index, scorable_index.last_updated
            )
        )
        equal, reason = test_utils.are_scorable_indexes_equal(
            expected_scorable_index, scorable_index
        )
        if not equal:
            self.fail(reason)
        self.assertTrue(
            scorable_index.last_updated - dt.datetime.utcnow()
            < dt.timedelta(seconds=30)
        )

    def test_huggingface_metadata(self):
        # Setup
        test_unique_id = "test_miner_id"
        miner = FakeMiner(storage=self.miner_storage)

        # Test upserting metadata
        metadata_to_upsert = [
            HuggingFaceMetadata(
                repo_name=f"test_repo_1_{test_unique_id}",  # Add unique_id to repo_name
                source=DataSource.REDDIT,
                updated_at=dt.datetime.utcnow()
            ),
            HuggingFaceMetadata(
                repo_name=f"test_repo_2_{test_unique_id}",  # Add unique_id to repo_name
                source=DataSource.X,
                updated_at=dt.datetime.utcnow()
            )
        ]

        # Store metadata in miner storage
        self.miner_storage.store_hf_dataset_info(metadata_to_upsert)

if __name__ == "__main__":
    unittest.main()