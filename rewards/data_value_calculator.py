import datetime as dt
from typing import Optional, List, Dict, Tuple
from common.data import DataSource, TimeBucket, DateRange
from common.data_v2 import ScorableDataEntityBucket
from rewards.data import DataDesirabilityLookup
from scraping.scraper import HFValidationResult
from rewards import data_desirability_lookup
from common import utils

class DataValueCalculator:
    """Calculates how rewards are distributed across DataSources and DataLabels."""
    
    def __init__(self, model: DataDesirabilityLookup = data_desirability_lookup.LOOKUP):
        # Convert to primitive version for performance optimization
        self.model = model.to_primitive_data_desirability_lookup()
    
    
    def get_score_for_data_entity_bucket(
        self,
        scorable_data_entity_bucket: ScorableDataEntityBucket,
        current_time_bucket: TimeBucket
    ) -> float:
        """Returns the score for the given data entity bucket."""
        # Extract frequently used values
        time_bucket_id = scorable_data_entity_bucket.time_bucket_id
        label = scorable_data_entity_bucket.label
        source = scorable_data_entity_bucket.source
        
        # Calculate time scalar
        time_scalar = self._scale_factor_for_age(time_bucket_id, current_time_bucket.id)
        if time_scalar == 0.0:
            return 0.0  # No need to do further processing
        
        # Find matching jobs directly using time bucket ID
        # Currently only finds matching jobs where keyword is None.
        matching_jobs = self.model.find_matching_jobs(source, None, label, time_bucket_id)
        
        # Rest of method remains the same...
        
        data_source_weight = self.model.get_data_source_weight(scorable_data_entity_bucket.source)
        
        if matching_jobs:
            # Calculate score based on matching jobs
            total_score = 0.0
            for job in matching_jobs:
                # Get job weight
                job_weight = job["job_weight"]
                
                # Calculate time scalar
                if job["start_timebucket"] or job["end_timebucket"]:
                    # For jobs with date constraints, if we've reached here, the time bucket
                    # overlaps with the job's date range, so use full time scalar of 1.0
                    time_scalar = 1.0
                else:
                    # For jobs without date constraints, use linear depreciation
                    time_scalar = self._scale_factor_for_age(
                        scorable_data_entity_bucket.time_bucket_id, 
                        current_time_bucket.id
                    )
                
                # Add this job's contribution to total score
                contribution = data_source_weight * job_weight * time_scalar * scorable_data_entity_bucket.scorable_bytes
                total_score += contribution
            
            return total_score
        else:
            # No matching jobs - use default scale factor
            default_scale_factor = self.model.get_default_scale_factor(scorable_data_entity_bucket.source)
            time_scalar = self._scale_factor_for_age(
                scorable_data_entity_bucket.time_bucket_id, 
                current_time_bucket.id
            )
            
            return (
                data_source_weight
                * default_scale_factor
                * time_scalar
                * scorable_data_entity_bucket.scorable_bytes
            )
    
    
    def _scale_factor_for_age(
        self, time_bucket_id: int, current_time_bucket_id: int
    ) -> float:
        """Returns the score scalar for data age.
        
        Uses a linear depreciation function:
        - Current data is scored 1.0
        - Data at max_age_in_hours is scored 0.5
        - Older data is scored 0.0
        """
        # Data age is scored using a linear depreciation function, where data from now is scored 1 and data
        # that is max_age_in_hours old is scored 0.5.
        # All data older than max_age_in_hours is scored 0.

        # Note: This makes the assumption that TimeBuckets are 1 hour buckets, which isn't ideal,
        # but we make the trade-off because it has a notable impact on perf vs. constructing TimeBuckets
        # to compute the age in hours.
        data_age_in_hours = current_time_bucket_id - time_bucket_id

        # Safe guard against future data.
        data_age_in_hours = max(0, data_age_in_hours)

        if data_age_in_hours > self.model.max_age_in_hours:
            return 0.0
        return 1.0 - (data_age_in_hours / (2 * self.model.max_age_in_hours))
    
