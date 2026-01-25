import bittensor as bt
import datetime as dt
import unittest
from common import constants
from common.data import (
    CompressedEntityBucket,
    CompressedMinerIndex,
    DataEntityBucket,
    DataEntity,
    DataEntityBucketId,
    DataLabel,
    TimeBucket,
    DataSource,
)
from common.data_v2 import ScorableDataEntityBucket, ScorableMinerIndex
from common.protocol import GetMinerIndex
import vali_utils.utils as vali_utils
import pytz
from common import utils


class TestValiUtils(unittest.TestCase):
    def test_choose_data_entity_bucket_to_query(self):
        """Calls choose_data_entity_bucket_to_query 10000 times and ensures the distribution of buckets chosen is as expected."""
        # We use scorable bytes when selecting, so keep size bytes identical.
        index = ScorableMinerIndex(
            hotkey="abc123",
            scorable_data_entity_buckets=[
                ScorableDataEntityBucket(
                    time_bucket_id=utils.time_bucket_id_from_datetime(
                        dt.datetime.now(tz=dt.timezone.utc)
                    ),
                    source=DataSource.REDDIT,
                    label="0",
                    size_bytes=300,
                    scorable_bytes=100,
                ),
                ScorableDataEntityBucket(
                    time_bucket_id=utils.time_bucket_id_from_datetime(
                        dt.datetime.now(tz=dt.timezone.utc)
                    ),
                    source=DataSource.REDDIT,
                    label="1",
                    size_bytes=300,
                    scorable_bytes=200,
                ),
                ScorableDataEntityBucket(
                    time_bucket_id=utils.time_bucket_id_from_datetime(
                        dt.datetime.now(tz=dt.timezone.utc)
                    ),
                    source=DataSource.REDDIT,
                    label="2",
                    size_bytes=300,
                    scorable_bytes=300,
                ),
            ],
            last_updated=dt.datetime.now(tz=dt.timezone.utc),
        )

        # Sample the buckets, counting how often each is chosen
        counts = [0, 0, 0]
        for _ in range(10000):
            chosen_bucket = vali_utils.choose_data_entity_bucket_to_query(index)
            self.assertIsInstance(chosen_bucket, DataEntityBucket)
            counts[int(chosen_bucket.id.label.value)] += 1

        total = sum(counts)
        ratios = [count / total for count in counts]
        self.assertAlmostEqual(ratios[0], 1 / 6, delta=0.05)
        self.assertAlmostEqual(ratios[1], 1 / 3, delta=0.05)
        self.assertAlmostEqual(ratios[2], 0.5, delta=0.05)

    def test_choose_entities_to_verify(self):
        """Calls choose_entity_to_verify 10000 times and verifies the distribution of entities chosen is as expected."""
        entities = [
            DataEntity(
                uri="uri1",
                datetime=dt.datetime.now(tz=dt.timezone.utc),
                source=DataSource.REDDIT,
                content=b"content1",
                content_size_bytes=100,
            ),
            DataEntity(
                uri="uri2",
                datetime=dt.datetime.now(tz=dt.timezone.utc),
                source=DataSource.REDDIT,
                content=b"content2",
                content_size_bytes=200,
            ),
            DataEntity(
                uri="uri3",
                datetime=dt.datetime.now(tz=dt.timezone.utc),
                source=DataSource.REDDIT,
                content=b"content3",
                content_size_bytes=300,
            ),
        ]

        # Sample the buckets, counting how often each is chosen
        counts = [0, 0, 0]
        for _ in range(10000):
            chosen_entities = vali_utils.choose_entities_to_verify(entities)
            # Expect exactly 2 samples are chosen each time.
            self.assertEqual(len(chosen_entities), 2)
            counts[entities.index(chosen_entities[0])] += 1
            counts[entities.index(chosen_entities[1])] += 1

        total = sum(counts)
        ratios = [count / total for count in counts]
        # The chance 1 is not picked: 0.58 (rounding) -> picked .42
        # 2 is picked first then 3 -> 2/6 * 3/4 = 0.25
        # 3 is picked first then 2 -> 3/6 * 2/3 = 0.33
        self.assertAlmostEqual(ratios[0], 0.42 / 2, delta=0.05)
        # The chance 2 is not picked: 0.27 (rounding) -> picked .73
        # 1 is picked first then 3 -> 1/6 * 3/5 = 0.1
        # 3 is picked first then 1 -> 3/6 * 1/3 = 0.16
        self.assertAlmostEqual(ratios[1], 0.73 / 2, delta=0.05)
        # The chance 3 is not picked: 0.15 (rounding) -> picked .85
        # 1 is picked first then 2 -> 1/6 * 2/5 = 0.06
        # 2 is picked first then 1 -> 2/6 * 1/4 = 0.08
        self.assertAlmostEqual(ratios[2], 0.85 / 2, delta=0.05)

    def test_choose_entities_to_verify_not_enough_entities(self):
        """Calls choose_entity_to_verify with only 1 entity and ensure we do not double count it."""
        entities = [
            DataEntity(
                uri="uri1",
                datetime=dt.datetime.now(tz=dt.timezone.utc),
                source=DataSource.REDDIT,
                content=b"content1",
                content_size_bytes=100,
            ),
        ]

        chosen_entities = vali_utils.choose_entities_to_verify(entities)

        self.assertEqual(len(chosen_entities), 1)

    def test_are_entities_valid_invalid_entities(self):
        """Tests a bunch of cases where the entities are invalid."""
        datetime = dt.datetime(2023, 12, 10, 12, 1, 0, tzinfo=dt.timezone.utc)
        default_label = DataLabel(value="label")
        default_data_entity_bucket = DataEntityBucket(
            id=DataEntityBucketId(
                time_bucket=TimeBucket.from_datetime(datetime),
                source=DataSource.REDDIT,
                label=default_label,
            ),
            size_bytes=10,
        )

        test_cases = [
            {
                "name": "Actual size doesn't match content size",
                "entities": [
                    DataEntity(
                        uri="http://1",
                        datetime=datetime,
                        source=DataSource.REDDIT,
                        label=default_label,
                        content=b"123",
                        content_size_bytes=3,
                    ),
                    DataEntity(
                        uri="http://2",
                        datetime=datetime,
                        source=DataSource.REDDIT,
                        label=default_label,
                        content=b"123",
                        content_size_bytes=200,  # Content size doesn't match the content
                    ),
                ],
                "data_entity_bucket": default_data_entity_bucket,
                "expected_error": "Size not as expected",
            },
            {
                "name": "Actual size less than bucket summary",
                "entities": [
                    DataEntity(
                        uri="http://1",
                        datetime=datetime,
                        source=DataSource.REDDIT,
                        label=default_label,
                        content=b"123",
                        content_size_bytes=3,
                    ),
                    DataEntity(
                        uri="http://2",
                        datetime=datetime,
                        source=DataSource.REDDIT,
                        label=default_label,
                        content=b"123",
                        content_size_bytes=3,
                    ),
                ],
                "data_entity_bucket": default_data_entity_bucket,
                "expected_error": "Size not as expected",
            },
            {
                "name": "Label doesn't match bucket summary",
                "entities": [
                    DataEntity(
                        uri="http://1",
                        datetime=datetime,
                        source=DataSource.REDDIT,
                        # No label
                        content=b"12345",
                        content_size_bytes=5,
                    ),
                    DataEntity(
                        uri="http://2",
                        datetime=datetime,
                        source=DataSource.REDDIT,
                        label=default_label,
                        content=b"12345",
                        content_size_bytes=5,
                    ),
                ],
                "data_entity_bucket": default_data_entity_bucket,
                "expected_error": "Entity label",
            },
            {
                "name": "DataSource doesn't match",
                "entities": [
                    DataEntity(
                        uri="http://1",
                        datetime=datetime,
                        source=DataSource.REDDIT,
                        label=default_label,
                        content=b"12345",
                        content_size_bytes=5,
                    ),
                    DataEntity(
                        uri="http://2",
                        datetime=datetime,
                        source=DataSource.X,
                        label=default_label,
                        content=b"12345",
                        content_size_bytes=5,
                    ),
                ],
                "data_entity_bucket": default_data_entity_bucket,
                "expected_error": "Entity source",
            },
            {
                "name": "Datetime before time_bucket",
                "entities": [
                    DataEntity(
                        uri="http://1",
                        datetime=datetime - dt.timedelta(hours=1),
                        source=DataSource.REDDIT,
                        label=default_label,
                        content=b"12345",
                        content_size_bytes=5,
                    ),
                    DataEntity(
                        uri="http://2",
                        datetime=datetime,
                        source=DataSource.REDDIT,
                        label=default_label,
                        content=b"12345",
                        content_size_bytes=5,
                    ),
                ],
                "data_entity_bucket": default_data_entity_bucket,
                "expected_error": "Entity datetime",
            },
            {
                "name": "Datetime after time_bucket",
                "entities": [
                    DataEntity(
                        uri="http://1",
                        datetime=datetime + dt.timedelta(hours=1),
                        source=DataSource.REDDIT,
                        label=default_label,
                        content=b"12345",
                        content_size_bytes=5,
                    ),
                    DataEntity(
                        uri="http://2",
                        datetime=datetime,
                        source=DataSource.REDDIT,
                        label=default_label,
                        content=b"12345",
                        content_size_bytes=5,
                    ),
                ],
                "data_entity_bucket": default_data_entity_bucket,
                "expected_error": "Entity datetime",
            },
        ]

        for test_case in test_cases:
            with self.subTest(test_case["name"], test_case=test_case):
                valid, reason = vali_utils.are_entities_valid(
                    test_case["entities"], test_case["data_entity_bucket"]
                )
                self.assertFalse(valid)
                self.assertRegex(reason, test_case["expected_error"])

    def test_are_entities_valid_valid_entities(self):
        """Tests are_entities_valid with valid entities."""
        datetime = dt.datetime(2023, 12, 10, 12, 1, 0, tzinfo=dt.timezone.utc)
        label = DataLabel(value="label")
        data_entity_bucket = DataEntityBucket(
            id=DataEntityBucketId(
                time_bucket=TimeBucket.from_datetime(datetime),
                source=DataSource.REDDIT,
                label=label,
            ),
            size_bytes=10,
        )
        entities = [
            DataEntity(
                uri="http://1",
                datetime=datetime,
                source=DataSource.REDDIT,
                label=label,
                content=b"12345",
                content_size_bytes=5,
            ),
            DataEntity(
                uri="http://2",
                datetime=datetime,
                source=DataSource.REDDIT,
                label=label,
                content=b"12345",
                content_size_bytes=5,
            ),
        ]
        valid, _ = vali_utils.are_entities_valid(entities, data_entity_bucket)
        self.assertTrue(valid)

    def test_are_entities_valid_non_utc_timezones(self):
        """Tests are_entities_valid with different timezones."""
        datetime = dt.datetime(
            2023, 12, 10, 12, 1, 0, tzinfo=pytz.timezone("America/Los_Angeles")
        )
        label = DataLabel(value="label")
        data_entity_bucket = DataEntityBucket(
            id=DataEntityBucketId(
                time_bucket=TimeBucket.from_datetime(datetime),
                source=DataSource.REDDIT,
                label=label,
            ),
            size_bytes=10,
        )
        entities = [
            DataEntity(
                uri="http://1",
                datetime=datetime,
                source=DataSource.REDDIT,
                label=label,
                content=b"12345",
                content_size_bytes=5,
            ),
            DataEntity(
                uri="http://2",
                datetime=datetime,
                source=DataSource.REDDIT,
                label=label,
                content=b"12345",
                content_size_bytes=5,
            ),
        ]
        valid, _ = vali_utils.are_entities_valid(entities, data_entity_bucket)
        self.assertTrue(valid)

    def test_are_entities_valid_no_timezones_default_utc(self):
        """Tests are_entities_valid with no timezones."""
        datetime_utc = dt.datetime(2023, 12, 10, 12, 1, 0, tzinfo=dt.timezone.utc)
        datetime = dt.datetime(2023, 12, 10, 12, 1, 0)
        label = DataLabel(value="label")
        data_entity_bucket = DataEntityBucket(
            id=DataEntityBucketId(
                time_bucket=TimeBucket.from_datetime(datetime_utc),
                source=DataSource.REDDIT,
                label=label,
            ),
            size_bytes=10,
        )
        entities = [
            DataEntity(
                uri="http://1",
                datetime=datetime,
                source=DataSource.REDDIT,
                label=label,
                content=b"12345",
                content_size_bytes=5,
            ),
            DataEntity(
                uri="http://2",
                datetime=datetime,
                source=DataSource.REDDIT,
                label=label,
                content=b"12345",
                content_size_bytes=5,
            ),
        ]
        valid, _ = vali_utils.are_entities_valid(entities, data_entity_bucket)
        self.assertTrue(valid)

    def test_are_entities_unique_unique_entities(self):
        """Tests are_entities_unique with unique entities."""
        datetime = dt.datetime(2023, 12, 10, 12, 1, 0, tzinfo=dt.timezone.utc)
        label = DataLabel(value="label")

        entities = [
            DataEntity(
                uri="http://1",
                datetime=datetime,
                source=DataSource.REDDIT,
                label=label,
                content=b"12345",
                content_size_bytes=5,
            ),
            DataEntity(
                uri="http://2",
                datetime=datetime,
                source=DataSource.REDDIT,
                label=label,
                content=b"67890",
                content_size_bytes=5,
            ),
        ]
        unique = vali_utils.are_entities_unique(entities)
        self.assertTrue(unique)

    def test_are_entities_unique_duplicate_entities(self):
        """Tests are_entities_unique with duplicate entities."""
        datetime = dt.datetime(2023, 12, 10, 12, 1, 0, tzinfo=dt.timezone.utc)
        label = DataLabel(value="label")

        entities = [
            DataEntity(
                uri="http://1",
                datetime=datetime,
                source=DataSource.REDDIT,
                label=label,
                content=b"12345",
                content_size_bytes=5,
            ),
            DataEntity(
                uri="http://1",
                datetime=datetime,
                source=DataSource.REDDIT,
                label=label,
                content=b"12345",
                content_size_bytes=5,
            ),
        ]
        unique = vali_utils.are_entities_unique(entities)
        self.assertFalse(unique)

    def test_are_entities_unique_duplicate_uris(self):
        """Tests that duplicated uris are not considered unique."""
        datetime = dt.datetime(2024, 3, 30, 2, 39, 43, tzinfo=dt.timezone.utc)
        label = DataLabel(value="#TikTok")

        entities = [
            DataEntity(
                uri="https://twitter.com/DorisCeciliaCa6/status/1773902901280129234",
                datetime=datetime,
                source=DataSource.X,
                label=label,
                content=b'{"username": "@DorisCeciliaCa6", "text": "\\ud83c\\udde8\\ud83c\\uddf1\\ud83c\\udde8\\ud83c\\uddf1\\ud83c\\udde8\\ud83c\\uddf1\\ud83c\\udde8\\ud83c\\uddf1\\ud83d\\udc4f\\ud83c\\udffc\\ud83d\\udc4f\\ud83c\\udffc\\ud83d\\udc4f\\ud83c\\udffc\\ud83d\\udc4f\\ud83c\\udffc\\ud83d\\udc4f\\ud83c\\udffcCheck out Amina\'s video! #TikTok", "url": "https://twitter.com/DorisCeciliaCa6/status/1773902901280129234", "timestamp": "2024-03-30T02:39:00.002112+00:00", "tweet_hashtags": ["#TikTok"], "model_config": {"extra": "ignore"}}',
                content_size_bytes=482,
            ),
            DataEntity(
                uri="https://twitter.com/DorisCeciliaCa6/status/1773902901280129234",
                datetime=datetime,
                source=DataSource.X,
                label=label,
                content=b'{"username": "@DorisCeciliaCa6", "text": "\\ud83c\\udde8\\ud83c\\uddf1\\ud83c\\udde8\\ud83c\\uddf1\\ud83c\\udde8\\ud83c\\uddf1\\ud83c\\udde8\\ud83c\\uddf1\\ud83d\\udc4f\\ud83c\\udffc\\ud83d\\udc4f\\ud83c\\udffc\\ud83d\\udc4f\\ud83c\\udffc\\ud83d\\udc4f\\ud83c\\udffc\\ud83d\\udc4f\\ud83c\\udffcCheck out Amina\'s video! #TikTok", "url": "https://twitter.com/DorisCeciliaCa6/status/1773902901280129234", "timestamp": "2024-03-30T02:39:00.002111+00:00", "tweet_hashtags": ["#TikTok"], "model_config": {"extra": "ignore"}}',
                content_size_bytes=482,
            ),
        ]
        self.assertFalse(vali_utils.are_entities_unique(entities))

    def test_are_entities_unique_duplicate_twitter_uris(self):
        """Tests that duplicated uris (with different twitter/x domains) are not considered unique."""
        datetime = dt.datetime(2024, 3, 30, 2, 39, 43, tzinfo=dt.timezone.utc)
        label = DataLabel(value="#TikTok")

        entities = [
            DataEntity(
                uri="https://x.com/DorisCeciliaCa6/status/1773902901280129234",
                datetime=datetime,
                source=DataSource.X,
                label=label,
                content=b'{"username": "@DorisCeciliaCa6", "text": "\\ud83c\\udde8\\ud83c\\uddf1\\ud83c\\udde8\\ud83c\\uddf1\\ud83c\\udde8\\ud83c\\uddf1\\ud83c\\udde8\\ud83c\\uddf1\\ud83d\\udc4f\\ud83c\\udffc\\ud83d\\udc4f\\ud83c\\udffc\\ud83d\\udc4f\\ud83c\\udffc\\ud83d\\udc4f\\ud83c\\udffc\\ud83d\\udc4f\\ud83c\\udffcCheck out Amina\'s video! #TikTok", "url": "https://x.com/DorisCeciliaCa6/status/1773902901280129234", "timestamp": "2024-03-30T02:39:00.002112+00:00", "tweet_hashtags": ["#TikTok"], "model_config": {"extra": "ignore"}}',
                content_size_bytes=482,
            ),
            DataEntity(
                uri="https://twitter.com/DorisCeciliaCa6/status/1773902901280129234",
                datetime=datetime,
                source=DataSource.X,
                label=label,
                content=b'{"username": "@DorisCeciliaCa6", "text": "\\ud83c\\udde8\\ud83c\\uddf1\\ud83c\\udde8\\ud83c\\uddf1\\ud83c\\udde8\\ud83c\\uddf1\\ud83c\\udde8\\ud83c\\uddf1\\ud83d\\udc4f\\ud83c\\udffc\\ud83d\\udc4f\\ud83c\\udffc\\ud83d\\udc4f\\ud83c\\udffc\\ud83d\\udc4f\\ud83c\\udffc\\ud83d\\udc4f\\ud83c\\udffcCheck out Amina\'s video! #TikTok", "url": "https://twitter.com/DorisCeciliaCa6/status/1773902901280129234", "timestamp": "2024-03-30T02:39:00.002111+00:00", "tweet_hashtags": ["#TikTok"], "model_config": {"extra": "ignore"}}',
                content_size_bytes=482,
            ),
        ]
        self.assertFalse(vali_utils.are_entities_unique(entities))

    def test_get_miner_index_from_response_compressed_index(self):
        """Tests get_miner_index_from_response with a compressed index."""

        compressed_index = CompressedMinerIndex(
            sources={
                DataSource.REDDIT: [
                    CompressedEntityBucket(
                        label="r/bittensor_",
                        time_bucket_ids=[5, 6],
                        sizes_bytes=[100, 200],
                    )
                ],
                DataSource.X: [
                    CompressedEntityBucket(
                        label="#bittensor",
                        time_bucket_ids=[6],
                        sizes_bytes=[300],
                    )
                ],
            }
        )

        response = GetMinerIndex(
            compressed_index_serialized=compressed_index.json(),
            dendrite=bt.TerminalInfo(status_code=200),
        )

        index = vali_utils.get_miner_index_from_response(response)
        self.assertEqual(index, compressed_index)

    def test_get_miner_index_from_response_new_index_bucket_size_too_large(self):
        """Tests get_miner_index_from_response with a compressed index that has a bucket that is too large."""

        compressed_index = CompressedMinerIndex(
            sources={
                DataSource.REDDIT: [
                    CompressedEntityBucket(
                        label="r/bittensor_",
                        time_bucket_ids=[5, 6],
                        sizes_bytes=[100, 200],
                    )
                ],
                DataSource.X: [
                    CompressedEntityBucket(
                        label="#bittensor",
                        time_bucket_ids=[6],
                        sizes_bytes=[constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES + 1],
                    )
                ],
            }
        )

        response = GetMinerIndex(
            compressed_index=compressed_index, dendrite=bt.TerminalInfo(status_code=200)
        )

        with self.assertRaises(ValueError):
            vali_utils.get_miner_index_from_response(response)


if __name__ == "__main__":
    unittest.main()
