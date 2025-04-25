import datetime as dt
from typing import Optional, Dict, List
from common.data import DataSource, TimeBucket
from common.data_v2 import ScorableDataEntityBucket
from rewards.data import DataDesirabilityLookup, JobLookup, Job


class DataValueCalculator:
    """Calculates how rewards are distributed across DataSources and DataLabels."""

    def __init__(self, model: JobLookup = None):
        self.model = DataDesirabilityLookup.to_primitive_data_desirability_lookup(model)
        self._create_lookup_dicts() #lookup dictionaries for faster access while scoring
        
        
    def _create_lookup_dicts(self):
        """Create lookup dictionaries for faster job matching."""
        # Group jobs by platform and topic for quicker lookup
        self.jobs_by_platform_topic: Dict[str, Dict[str, List[Job]]] = {}
        
        for job in self.model.job_list:
            platform = job.params.platform
            topic = job.params.topic
            
            if platform not in self.jobs_by_platform_topic:
                self.jobs_by_platform_topic[platform] = {}
                
            if topic not in self.jobs_by_platform_topic[platform]:
                self.jobs_by_platform_topic[platform][topic] = []
                
            self.jobs_by_platform_topic[platform][topic].append(job)


    def _calculate_job_score(
        self, 
        job: Job, 
        data_datetime: dt.datetime,
        current_time_bucket: TimeBucket,
        data_time_bucket_id: int,
        scorable_bytes: int
    ) -> float:
        """Calculate score based on job parameters and data attributes."""
        # Check time range if specified
        if job.params.post_start_datetime or job.params.post_end_datetime:
            # Convert ISO string to datetime
            start_time = None
            if job.params.post_start_datetime:
                start_time = dt.datetime.fromisoformat(
                    job.params.post_start_datetime.replace('Z', '+00:00')
                )
            
            end_time = None
            if job.params.post_end_datetime:
                end_time = dt.datetime.fromisoformat(
                    job.params.post_end_datetime.replace('Z', '+00:00')
                )
            
            # Check if data is within time range
            if (start_time and data_datetime < start_time) or \
               (end_time and data_datetime > end_time):
                return 0.0
            
            # If within range, reward at 0.5 (subject to change)
            return job.weight * scorable_bytes * 0.5
        else:
            # If no time range specified, use age-based scaling
            time_scalar = self._scale_factor_for_age(
                data_time_bucket_id, 
                current_time_bucket.id
            )
            return job.weight * time_scalar * scorable_bytes


    def get_score_for_data_entity_bucket(
        self,
        scorable_data_entity_bucket: ScorableDataEntityBucket,
        current_time_bucket: TimeBucket
    ) -> float:
        """Returns the score for the given data entity bucket.
        
        A data entity bucket is scored as follows:
        1. Weighted based on the weight of its platform.
        2. Scaled based on matching jobs for the platform and label/topic.
        3. Scaled based on the age of the data or time ranges specified in jobs.
        """
        # Map DataSource to platform string
        platform = scorable_data_entity_bucket.source
        
        # If this platform isn't in our model, return 0
        if platform not in self.model.platform_weights:
            return 0.0
            
        # Get the platform (data source) weight
        platform_weight = self.model.platform_weights[platform]
        
        # Find jobs that match this platform and label
        label = scorable_data_entity_bucket.label
        
        # If no label, use default weight * platform weight
        if not label:
            default_weight = self.model.default_label_weight if platform else 0
            time_scalar = self._scale_factor_for_age(
                scorable_data_entity_bucket.time_bucket_id, 
                current_time_bucket.id
            )
            return default_weight * platform_weight * time_scalar * scorable_data_entity_bucket.scorable_bytes
        
        # If this platform/label combination doesn't match any jobs
        if (platform not in self.jobs_by_platform_topic or 
            label not in self.jobs_by_platform_topic[platform]):
            default_weight = self.model.default_label_weight
            time_scalar = self._scale_factor_for_age(
                scorable_data_entity_bucket.time_bucket_id, 
                current_time_bucket.id
            )
            return default_weight * platform_weight * time_scalar * scorable_data_entity_bucket.scorable_bytes
        
        matching_jobs = self.jobs_by_platform_topic[platform][label]
        
        # Calculate time bucket datetime
        time_bucket_datetime = TimeBucket.to_datetime(scorable_data_entity_bucket.time_bucket_id)
        
        best_score = 0.0
        
        # Check each matching job and find the best score
        for job in matching_jobs:
            job_score = self._calculate_job_score(
                job, 
                time_bucket_datetime,
                current_time_bucket,
                scorable_data_entity_bucket.time_bucket_id,
                scorable_data_entity_bucket.scorable_bytes
            )
            best_score = max(best_score, job_score)     #TODO: max score or sum of scores?
        
        # Apply platform weight
        return platform_weight * best_score

    def _scale_factor_for_source_and_label(
        self, data_source: DataSource, label: Optional[str]
    ) -> float:
        """Returns the score scalar for the given data source and label."""
        data_source_reward = self.model.distribution[data_source]
        label_factor = data_source_reward.label_scale_factors.get(
            label, data_source_reward.default_scale_factor
        )
        return data_source_reward.weight * label_factor


    def _scale_factor_for_age(
        self, time_bucket_id: int, current_time_bucket_id: int
    ) -> float:
        """Returns the score scalar for data ."""
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
