import unittest
import json
from datetime import datetime, timedelta
from typing import Dict, Any
from common.data import DataSource
from rewards.data import DataDesirabilityLookup, DataSourceDesirability, Job, JobMatcher
from dynamic_desirability.desirability_retrieval import to_lookup

class TestDataDesirabilityLookupConversion(unittest.TestCase):
    """Test cases for converting JSON data into a DataDesirabilityLookup object."""

    def setUp(self):
        """Set up test data."""
        # Create a sample JSON representation of jobs
        self.now = datetime.now().replace(microsecond=0).isoformat()
        self.future = (datetime.now() + timedelta(days=7)).replace(microsecond=0).isoformat()
        
        self.sample_json = {
            "jobs": [
                {
                    "id": "label_job_1",
                    "weight": 2.5,
                    "params": {
                        "job_type": "label",
                        "platform": "reddit",
                        "topic": "technology"
                    }
                },
                {
                    "id": "keyword_job_1",
                    "weight": 1.8,
                    "params": {
                        "job_type": "keyword",
                        "platform": "x",
                        "topic": "ai",
                        "post_start_datetime": self.now,
                        "post_end_datetime": self.future
                    }
                },
                {
                    "id": "label_job_2",
                    "weight": 3.0,
                    "params": {
                        "job_type": "label",
                        "platform": "reddit",
                        "topic": "science"
                    }
                }
            ]
        }
        
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
        reddit_job_topics = {job.topic for job in reddit_jobs}
        self.assertIn("technology", reddit_job_topics)
        self.assertIn("science", reddit_job_topics)
        
        # Find a specific job and verify its weight
        tech_job = next((job for job in reddit_jobs if job.topic == "technology"), None)
        self.assertIsNotNone(tech_job)
        self.assertEqual(tech_job.job_weight, 2.5)
        
        # Verify X jobs
        x_jobs = lookup.distribution[DataSource.X].job_matcher.jobs
        self.assertEqual(len(x_jobs), 1)
        
        # Verify job with date constraints
        ai_job = x_jobs[0]
        self.assertEqual(ai_job.topic, "ai")
        self.assertEqual(ai_job.job_type, "keyword")
        self.assertEqual(ai_job.start_datetime, self.now)
        self.assertEqual(ai_job.end_datetime, self.future)
        
    def test_job_matching(self):
        """Test job matching functionality in the resulting lookup."""
        
        # Convert JSON to lookup
        lookup = to_lookup("test_jobs.json")
        
        # Convert to primitive for performance testing
        primitive_lookup = lookup.to_primitive_data_desirability_lookup()
        
        # Test matching a Reddit job
        now = datetime.now().isoformat()
        after_now = (datetime.now() + timedelta(hours=1)).isoformat()
        
        # Should match the technology job
        reddit_jobs = primitive_lookup.find_matching_jobs(
            DataSource.REDDIT, "label", "technology", (now, after_now)
        )
        self.assertEqual(len(reddit_jobs), 1)
        self.assertEqual(reddit_jobs[0]["topic"], "technology")
        
        # Should not match a non-existent topic
        no_jobs = primitive_lookup.find_matching_jobs(
            DataSource.REDDIT, "label", "nonexistent", (now, after_now)
        )
        self.assertEqual(len(no_jobs), 0)
        
        # Test date-constrained job matching
        # Within date range
        x_jobs_1 = primitive_lookup.find_matching_jobs(
            DataSource.X, "keyword", "ai", (now, after_now)
        )
        self.assertEqual(len(x_jobs_1), 1)
        
        # Before date range (should not match)
        before_now = (datetime.now() - timedelta(days=10)).isoformat()
        even_before = (datetime.now() - timedelta(days=9)).isoformat()
        x_jobs_2 = primitive_lookup.find_matching_jobs(
            DataSource.X, "keyword", "ai", (before_now, even_before)
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