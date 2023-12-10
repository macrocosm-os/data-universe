import datetime as dt
import unittest
from common.data import (
    DataChunkSummary,
    DataEntity,
    DataLabel,
    TimeBucket,
    DataSource,
    ScorableDataChunkSummary,
    ScorableMinerIndex,
)
from neurons.validator import Validator


class TestValidator(unittest.TestCase):
    def test_choose_chunk_to_query(self):
        """Calls choose_chunk_to_query 10000 times and ensures the distribution of chunks chosen is as expected."""
        index = ScorableMinerIndex(
            hotkey="abc123",
            scorable_chunks=[
                ScorableDataChunkSummary(
                    time_bucket=TimeBucket.from_datetime(
                        dt.datetime.now(tz=dt.timezone.utc)
                    ),
                    source=DataSource.REDDIT,
                    size_bytes=100,
                    scorable_bytes=100,
                ),
                ScorableDataChunkSummary(
                    time_bucket=TimeBucket.from_datetime(
                        dt.datetime.now(tz=dt.timezone.utc)
                    ),
                    source=DataSource.REDDIT,
                    size_bytes=200,
                    scorable_bytes=200,
                ),
                ScorableDataChunkSummary(
                    time_bucket=TimeBucket.from_datetime(
                        dt.datetime.now(tz=dt.timezone.utc)
                    ),
                    source=DataSource.REDDIT,
                    size_bytes=300,
                    scorable_bytes=300,
                ),
            ],
            last_updated=dt.datetime.now(tz=dt.timezone.utc),
        )

        # Sample the chunks, counting how often each is chosen
        counts = [0, 0, 0]
        for _ in range(10000):
            chosen_chunk = Validator.choose_chunk_to_query(index)
            self.assertIsInstance(chosen_chunk, DataChunkSummary)
            counts[index.scorable_chunks.index(chosen_chunk)] += 1

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

        # Sample the chunks, counting how often each is chosen
        counts = [0, 0, 0]
        for _ in range(10000):
            chosen_entities = Validator.choose_entities_to_verify(entities)
            # Expect only a single sample to be chosen.
            self.assertEqual(len(chosen_entities), 1)
            counts[entities.index(chosen_entities[0])] += 1

        total = sum(counts)
        ratios = [count / total for count in counts]
        self.assertAlmostEqual(ratios[0], 1 / 6, delta=0.05)
        self.assertAlmostEqual(ratios[1], 1 / 3, delta=0.05)
        self.assertAlmostEqual(ratios[2], 0.5, delta=0.05)

    def test_are_entities_valid_invalid_entities(self):
        """Tests a bunch of bases where the entities are invalid."""
        datetime = dt.datetime(2023, 12, 10, 12, 1, 0, tzinfo=dt.timezone.utc)
        default_label = DataLabel(value="label")
        default_chunk_summary = DataChunkSummary(
            time_bucket=TimeBucket.from_datetime(datetime),
            source=DataSource.REDDIT,
            label=default_label,
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
                "chunk": default_chunk_summary,
                "expected_error": "Size not as expected",
            },
            {
                "name": "Actual size less than chunk summary",
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
                "chunk": default_chunk_summary,
                "expected_error": "Size not as expected",
            },
            {
                "name": "Label doesn't match chunk summary",
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
                "chunk": default_chunk_summary,
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
                "chunk": default_chunk_summary,
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
                "chunk": default_chunk_summary,
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
                "chunk": default_chunk_summary,
                "expected_error": "Entity datetime",
            },
        ]

        for test_case in test_cases:
            with self.subTest(test_case["name"], test_case=test_case):
                valid, reason = Validator.are_entities_valid(
                    test_case["entities"], test_case["chunk"]
                )
                self.assertFalse(valid)
                self.assertRegex(reason, test_case["expected_error"])

    def test_are_entities_valid_valid_entities(self):
        """Tests are_entities_valid with valid entities."""
        datetime = dt.datetime(2023, 12, 10, 12, 1, 0, tzinfo=dt.timezone.utc)
        label = DataLabel(value="label")
        chunk_summary = DataChunkSummary(
            time_bucket=TimeBucket.from_datetime(datetime),
            source=DataSource.REDDIT,
            label=label,
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
        valid, _ = Validator.are_entities_valid(entities, chunk_summary)
        self.assertTrue(valid)


if __name__ == "__main__":
    unittest.main()
