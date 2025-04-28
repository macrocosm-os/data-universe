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
        """Returns the score for the given data entity bucket.
        
        A data entity bucket is scored as follows:
        1. If there are matching jobs, score is based on the sum of those job weights 
           multiplied by data source weight
        2. If there are no matching jobs, score uses default scale factor
        3. All scores are scaled based on data age if applicable
        """
        # Convert time_bucket to daterange for job matching
        bucket_daterange = self._time_bucket_to_iso_daterange(scorable_data_entity_bucket.time_bucket_id)
        
        # Always use "label" as the job_type
        job_type = "label"
        topic = scorable_data_entity_bucket.label
        
        # Find matching jobs
        matching_jobs = self.model.find_matching_jobs(
            scorable_data_entity_bucket.source, 
            job_type, 
            topic, 
            bucket_daterange
        )
        
        data_source_weight = self.model.get_data_source_weight(scorable_data_entity_bucket.source)
        
        if matching_jobs:
            # Calculate score based on matching jobs
            total_score = 0.0
            for job in matching_jobs:
                # Get job weight
                job_weight = job["job_weight"]
                
                # Calculate time scalar
                if job["start_date"] or job["end_datetime"]:
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
    
    def _time_bucket_to_iso_daterange(self, time_bucket_id: int) -> Tuple[str, str]:
        """Converts a time bucket ID to an ISO formatted date range tuple."""
        start_dt = utils.datetime_from_hours_since_epoch(time_bucket_id)
        end_dt = utils.datetime_from_hours_since_epoch(time_bucket_id + 1)
        return (start_dt.isoformat(), end_dt.isoformat())
    
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
    