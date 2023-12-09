import datetime as dt
import unittest
from common.data import (
    DataChunkSummary,
    DataEntity,
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


if __name__ == "__main__":
    unittest.main()
