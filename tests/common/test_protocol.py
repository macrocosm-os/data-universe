import unittest
import datetime as dt
from common.data import DataEntity, DataEntityBucketId, DataLabel, DataSource, TimeBucket

from common.protocol import GetDataEntityBucket, GetMinerIndex


class TestGetMinerIndex(unittest.TestCase):
    def test_synapse_serialization(self):
        """Tests that the protocol messages can be serialized/deserialized for transport."""
        request = GetMinerIndex()
        json = request.json()
        deserialized = GetMinerIndex.parse_raw(json)
        self.assertEqual(request, deserialized)
        
        # Also check that the headers can be constructed.
        request.to_headers()
        
class TestGetDataEntityBucket(unittest.TestCase):
    def test_synapse_serialization(self):
        """Tests that the protocol messages can be serialized/deserialized for transport."""
        request = GetDataEntityBucket(data_entity_bucket_id=DataEntityBucketId(
            time_bucket=TimeBucket.from_datetime(dt.datetime.utcnow()), 
            label=DataLabel(value="r/bittensor_"),
            source=DataSource.REDDIT))
        json = request.json()
        deserialized = GetDataEntityBucket.parse_raw(json)
        self.assertEqual(request, deserialized)
        
        # Also check that the headers can be constructed.
        request.to_headers()
    

if __name__ == "__main__":
    unittest.main()