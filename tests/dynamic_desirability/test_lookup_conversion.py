import unittest
import json
from datetime import datetime, timedelta
from typing import Dict, Any
from common.data import DataSource
from rewards.data import DataDesirabilityLookup, DataSourceDesirability, Job, JobMatcher
from dynamic_desirability.desirability_retrieval import to_lookup, datetime_to_timebucket
from common import utils

class TestDataDesirabilityLookupConversion(unittest.TestCase):
    """Test cases for converting JSON data into a DataDesirabilityLookup object."""

    def setUp(self):
        """Set up test data."""
        # Create a sample JSON representation of jobs
        self.now = datetime.now().replace(microsecond=0)
        self.future = (datetime.now() + timedelta(days=7)).replace(microsecond=0)
        
        self.now_str = self.now.isoformat()
        self.future_str = self.future.isoformat()
        
        # Convert to time buckets for later comparison
        self.now_timebucket = utils.time_bucket_id_from_datetime(self.now)
        self.future_timebucket = utils.time_bucket_id_from_datetime(self.future)
        
        self.sample_json = [
            {
                "id": "label_job_1",
                "weight": 2.5,
                "params": {
                    "keyword": None,  
                    "platform": "reddit",
                    "label": "technology"  
                }
            },
            {
                "id": "keyword_job_1",
                "weight": 1.8,
                "params": {
                    "keyword": "ai", 
                    "platform": "x",
                    "label": "ai_news",  
                    "post_start_datetime": self.now_str,
                    "post_end_datetime": self.future_str
                }
            },
            {
                "id": "label_job_2",
                "weight": 3.0,
                "params": {
                    "keyword": None,  
                    "platform": "reddit",
                    "label": "science" 
                }
            }
        ]
        
        # Write JSON to a temporary file
        with open("test_jobs.json", "w") as f:
            json.dump(self.sample_json, f)

    def test_json_to_lookup(self):
        """Test converting JSON to DataDesirabilityLookup."""
        
        # Convert JSON to lookup
        lookup = to_lookup("test_jobs.json")
        
        # Basic validation that the lookup was created
        self.assertIsInstance(lookup, DataDesirabilityLookup)
        
        # Verify that both platforms are in the distribution
        self.assertIn(DataSource.REDDIT, lookup.distribution)
        self.assertIn(DataSource.X, lookup.distribution)
        
        # Verify source weights
        self.assertEqual(lookup.distribution[DataSource.REDDIT].weight, DataSource.REDDIT.weight)
        self.assertEqual(lookup.distribution[DataSource.X].weight, DataSource.X.weight)

        # Verify Reddit jobs
        reddit_jobs = lookup.distribution[DataSource.REDDIT].job_matcher.jobs
        self.assertEqual(len(reddit_jobs), 2)
        
        # Verify specific job properties
        reddit_job_labels = {job.label for job in reddit_jobs}
        self.assertIn("technology", reddit_job_labels)
        self.assertIn("science", reddit_job_labels)
        
        # Find a specific job and verify its weight
        tech_job = next((job for job in reddit_jobs if job.label == "technology"), None)
        self.assertIsNotNone(tech_job)
        self.assertEqual(tech_job.job_weight, 2.5)
        self.assertIsNone(tech_job.keyword)  # keyword should be None
        
        # Verify X jobs
        x_jobs = lookup.distribution[DataSource.X].job_matcher.jobs
        self.assertEqual(len(x_jobs), 1)
        
        # Verify job with date constraints
        ai_job = x_jobs[0]
        self.assertEqual(ai_job.label, "ai_news")
        self.assertEqual(ai_job.keyword, "ai")  # should have "ai" as keyword
        self.assertEqual(ai_job.start_timebucket, self.now_timebucket)  # using timebucket instead of datetime
        self.assertEqual(ai_job.end_timebucket, self.future_timebucket)  # using timebucket instead of datetime
        
    def test_job_matching(self):
        """Test job matching functionality in the resulting lookup."""
        
        # Convert JSON to lookup
        lookup = to_lookup("test_jobs.json")
        
        # Convert to primitive for performance testing
        primitive_lookup = lookup.to_primitive_data_desirability_lookup()
        
        # Get current time bucket for testing
        now_dt = datetime.now()
        now_timebucket = utils.time_bucket_id_from_datetime(now_dt)
        
        # Should match the technology job
        reddit_jobs = primitive_lookup.find_matching_jobs(
            DataSource.REDDIT, None, "technology", now_timebucket  # Using time bucket ID directly
        )
        self.assertEqual(len(reddit_jobs), 1)
        self.assertEqual(reddit_jobs[0]["label"], "technology")
        
        # Should not match a non-existent label
        no_jobs = primitive_lookup.find_matching_jobs(
            DataSource.REDDIT, None, "nonexistent", now_timebucket  # Using time bucket ID directly
        )
        self.assertEqual(len(no_jobs), 0)
        
        # Test date-constrained job matching
        # Within date range
        x_jobs_1 = primitive_lookup.find_matching_jobs(
            DataSource.X, "ai", "ai_news", now_timebucket  # Using time bucket ID directly
        )
        self.assertEqual(len(x_jobs_1), 1)
        
        # Before date range (should not match)
        before_now_dt = datetime.now() - timedelta(days=10)
        before_now_timebucket = utils.time_bucket_id_from_datetime(before_now_dt)
        x_jobs_2 = primitive_lookup.find_matching_jobs(
            DataSource.X, "ai", "ai_news", before_now_timebucket  # Using time bucket ID directly
        )
        self.assertEqual(len(x_jobs_2), 0)
    
    def test_default_scale_factor(self):
        """Test that default scale factors are correctly set."""
        
        # Convert JSON to lookup
        lookup = to_lookup("test_jobs.json")
        primitive_lookup = lookup.to_primitive_data_desirability_lookup()
        
        # Check default scale factors
        default_reddit = primitive_lookup.get_default_scale_factor(DataSource.REDDIT)
        self.assertEqual(default_reddit, 0.3)  # Default value from the code
        
        # Test source weight retrieval
        weight_reddit = primitive_lookup.get_data_source_weight(DataSource.REDDIT)
        self.assertEqual(weight_reddit, DataSource.REDDIT.weight)
    
    def test_error_handling(self):
        """Test error handling for invalid JSON."""
        with open("invalid_jobs.json", "w") as f:
            f.write("{invalid json")
        
        with self.assertRaises(Exception):
            lookup = to_lookup("invalid_jobs.json")
    
    def tearDown(self):
        """Clean up test files."""
        import os
        # Remove temporary files
        try:
            os.remove("test_jobs.json")
        except:
            pass
        try:
            os.remove("invalid_jobs.json")
        except:
            pass


if __name__ == "__main__":
    unittest.main()