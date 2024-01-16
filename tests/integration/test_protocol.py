from typing import Callable, Tuple
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
    MinerIndex,
    TimeBucket,
)

from common.protocol import GetDataEntityBucket, GetMinerIndex
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
                self.storage.get_compressed_index().model_dump_json()
            )
            return request
        return self.index

    def get_data_bucket(
        self, request: old_protocol.GetDataEntityBucket
    ) -> old_protocol.GetDataEntityBucket:
        return self.entities


class IntegrationTestProtocol(unittest.TestCase):
    # The commented out tests run a mini E2E test for round trip serialization of a protocol message.
    # To run it, first make sure you have a wallet named "unit_test" with hotkey "unit_test".
    # Then update the axon info to use the correct addresses for your wallets.

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

    def _test_round_trip(self, forward_fn: Callable, request: bt.Synapse) -> bt.Synapse:
        """Base test for verifying a protocol message between dendrite and axon.

        Args:
            - forward_fn: The function that handles the request on the axon.
            - request: The request to send to the axon.
        """

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

            request = GetMinerIndex()
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

    def _create_test_indexes(self) -> Tuple[MinerIndex, CompressedMinerIndex]:
        """Returns a tuple of a MinerIndex and its equivalent CompressedMinerIndex for testing."""
        index = MinerIndex(
            hotkey=self.wallet.hotkey.ss58_address,
            data_entity_buckets=[
                DataEntityBucket(
                    id=DataEntityBucketId(
                        time_bucket=TimeBucket(id=1),
                        label=DataLabel(value="r/bittensor_"),
                        source=DataSource.REDDIT,
                    ),
                    size_bytes=100,
                ),
                DataEntityBucket(
                    id=DataEntityBucketId(
                        time_bucket=TimeBucket(id=2),
                        label=DataLabel(value="r/bittensor_"),
                        source=DataSource.REDDIT,
                    ),
                    size_bytes=200,
                ),
                DataEntityBucket(
                    id=DataEntityBucketId(
                        time_bucket=TimeBucket(id=3),
                        label=DataLabel(value="r/bittensor_"),
                        source=DataSource.REDDIT,
                    ),
                    size_bytes=300,
                ),
                DataEntityBucket(
                    id=DataEntityBucketId(
                        time_bucket=TimeBucket(id=3),
                        source=DataSource.X,
                    ),
                    size_bytes=123,
                ),
                DataEntityBucket(
                    id=DataEntityBucketId(
                        time_bucket=TimeBucket(id=4),
                        source=DataSource.X,
                    ),
                    size_bytes=234,
                ),
                DataEntityBucket(
                    id=DataEntityBucketId(
                        time_bucket=TimeBucket(id=5),
                        label=DataLabel(value="#bittensor"),
                        source=DataSource.X,
                    ),
                    size_bytes=321,
                ),
                DataEntityBucket(
                    id=DataEntityBucketId(
                        time_bucket=TimeBucket(id=4),
                        label=DataLabel(value="#bittensor"),
                        source=DataSource.X,
                    ),
                    size_bytes=99,
                ),
            ],
        )

        compressed_index = CompressedMinerIndex(
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

        return (index, compressed_index)

    def test_get_miner_index(self):
        """Tests a round trip using the new compressed miner format."""

        # TODO: Eventually this should write entries to the FakeMiner's storage
        # and then have the FakeMiner read them back out. For now, we just create
        # the response the miner will return directly.

        index, compressed_index = self._create_test_indexes()

        expected_response = GetMinerIndex(
            compressed_index_serialized=compressed_index.model_dump_json(),
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

        # TODO: Refactor the vali so that we can directly use vali code here.
        # Now that we have the response, write it t othe vali DB.
        got_compressed_index = vali_utils.get_miner_index_from_response(
            response, self.wallet.hotkey.ss58_address
        )
        self.assertEqual(got_compressed_index, compressed_index)

        # Store in the vali DB.
        self.vali_storage.upsert_compressed_miner_index(
            got_compressed_index, self.wallet.hotkey.ss58_address, 1
        )

        # Finally, read it out and verify it's as expected.
        scorable_index = self.vali_storage.read_miner_index(
            self.wallet.hotkey.ss58_address
        )
        expected_scorable_index = test_utils.convert_to_scorable_miner_index(
            index, scorable_index.last_updated
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

        # TODO: Refactor the vali so that we can directly use vali code here.
        # Now that we have the response, write it t othe vali DB.
        got_compressed_index = vali_utils.get_miner_index_from_response(
            response, self.wallet.hotkey.ss58_address
        )
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


if __name__ == "__main__":
    unittest.main()
