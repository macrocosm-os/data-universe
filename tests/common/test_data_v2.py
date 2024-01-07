import unittest
from common.data import DataEntityBucketId, DataLabel, DataSource, TimeBucket
from common.data_v2 import ScorableDataEntityBucket, DataEntityBucket


class TestDataV2(unittest.TestCase):
    def test_scorable_data_entity_bucket_to_data_entity(self):
        # Create a ScorableDataEntityBucket instance
        time_bucket_id = 123
        source = DataSource.REDDIT.value
        label = "EXAMPLE_label"
        size_bytes = 1000
        scorable_bytes = 500
        scorable_data_entity_bucket = ScorableDataEntityBucket(
            time_bucket_id=time_bucket_id,
            source=source,
            label=label,
            size_bytes=size_bytes,
            scorable_bytes=scorable_bytes,
        )

        # Call the to_data_entity_bucket method
        data_entity_bucket = scorable_data_entity_bucket.to_data_entity_bucket()

        # Verify that the returned value is an instance of DataEntityBucket
        expected = DataEntityBucket(
            id=DataEntityBucketId(
                time_bucket=TimeBucket(id=time_bucket_id),
                source=source,
                label=DataLabel(value=label.casefold()),
            ),
            size_bytes=size_bytes,
        )
        self.assertEqual(data_entity_bucket, expected)

    def test_scorable_data_entity_bucket_to_data_entity_none_label(self):
        # Create a ScorableDataEntityBucket instance
        time_bucket_id = 123
        source = DataSource.REDDIT.value
        size_bytes = 1000
        scorable_bytes = 500
        scorable_data_entity_bucket = ScorableDataEntityBucket(
            time_bucket_id=time_bucket_id,
            source=source,
            label=None,
            size_bytes=size_bytes,
            scorable_bytes=scorable_bytes,
        )

        # Call the to_data_entity_bucket method
        data_entity_bucket = scorable_data_entity_bucket.to_data_entity_bucket()

        # Verify that the returned value is an instance of DataEntityBucket
        expected = DataEntityBucket(
            id=DataEntityBucketId(
                time_bucket=TimeBucket(id=time_bucket_id),
                source=source,
                label=None,
            ),
            size_bytes=size_bytes,
        )
        self.assertEqual(data_entity_bucket, expected)

    def test_scorable_data_entity_bucket_equality(self):
        # Create two ScorableDataEntityBucket instances
        time_bucket_id = 123
        source = DataSource.REDDIT.value
        label = "EXAMPLE_label"
        size_bytes = 1000
        scorable_bytes = 500
        scorable_data_entity_bucket_1 = ScorableDataEntityBucket(
            time_bucket_id=time_bucket_id,
            source=source,
            label=label,
            size_bytes=size_bytes,
            scorable_bytes=scorable_bytes,
        )
        scorable_data_entity_bucket_2 = ScorableDataEntityBucket(
            time_bucket_id=time_bucket_id,
            source=source,
            label=label,
            size_bytes=size_bytes,
            scorable_bytes=scorable_bytes,
        )

        # Verify that the two instances are equal
        self.assertEqual(scorable_data_entity_bucket_1, scorable_data_entity_bucket_2)


if __name__ == "__main__":
    unittest.main()
