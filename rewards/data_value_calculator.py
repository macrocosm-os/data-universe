import datetime as dt
from typing import Optional
from common.data import DataSource, TimeBucket
from common.data_v2 import ScorableDataEntityBucket
from rewards.data import DynamicDesirabilityLookup
from scraping.scraper import HFValidationResult
from dynamic_desirability.desirability_retrieval import to_lookup

class DataValueCalculator:
    """Calculates how rewards are distributed across DataSources and DataLabels."""

    def __init__(self, model: Optional[DynamicDesirabilityLookup]):
        self.model = DynamicDesirabilityLookup.to_primitive_dynamic_desirability_lookup(model) if model else None

    def get_score_for_data_entity_bucket(
        self,
        scorable_data_entity_bucket: ScorableDataEntityBucket,
        current_time_bucket: TimeBucket
    ) -> float:
        """Returns the score for the given data entity bucket.

        A data entity bucket is scored as follows:
            1. It is weighted based on the weight of its data source.
            2. It's scaled based on the Label. This may be negative if the data is undesirable.
            3. It's scaled based on the age of the data, where newer data is considered more valuable.
        """

        data_type_scale_factor = self._scale_factor_for_source_and_label(
            scorable_data_entity_bucket.source, scorable_data_entity_bucket.label
        )
        time_scalar = self._scale_factor_for_age(
            scorable_data_entity_bucket.source, 
            scorable_data_entity_bucket.label, 
            scorable_data_entity_bucket.time_bucket_id, 
            current_time_bucket.id
        )

        return (
            data_type_scale_factor
            * time_scalar
            * scorable_data_entity_bucket.scorable_bytes
        )

    def _scale_factor_for_source_and_label(
        self, data_source: DataSource, label: Optional[str]
    ) -> float:
        """Returns the score scalar for the given data source and label."""
        data_source_reward = self.model.distribution[data_source]

        if label and label in data_source_reward.label_config:
            label_factor = data_source_reward.label_config[label][0]
        else:
            label_factor = data_source_reward.default_scale_factor
        
        return data_source_reward.weight * label_factor

    def _scale_factor_for_age(
        self, data_source: DataSource, label: Optional[str], time_bucket_id: int, current_time_bucket_id: int
    ) -> float:
        """Returns the score scalar for data based on its age and optional earliest viable date."""
        data_age_in_hours = current_time_bucket_id - time_bucket_id
        
        # Safeguard against future data
        data_age_in_hours = max(0, data_age_in_hours)
        
        # If the label exists in the model's label_config
        if label and label in self.model.distribution[data_source].label_config:
            _, earliest_viable_datetime_str = self.model.distribution[data_source].label_config[label]

            if earliest_viable_datetime_str:
                # Convert earliest_viable_datetime_str to a time bucket ID
                earliest_viable_datetime = dt.datetime.fromisoformat(earliest_viable_datetime_str)
                earliest_viable_datetime = earliest_viable_datetime.astimezone(dt.timezone.utc)
                earliest_viable_bucket_id = earliest_viable_datetime.timestamp() // 3600 # seconds to hours
                
                # If the data time bucket is from after or equal to the earliest viable time bucket
                if time_bucket_id >= earliest_viable_bucket_id:
                    return 0.3 # some constant 
                else:
                    return 0.0  # data is from before earliest viable datetime
        
        # Regular age-based scoring
        # Data age is scored using a linear depreciation function, where data from now is scored 1 and data
        # that is max_age_in_hours old is scored 0.5.
        # All data older than max_age_in_hours is scored 0.
        if data_age_in_hours > self.model.max_age_in_hours:
            return 0.0
        
        return 1.0 - (data_age_in_hours / (2 * self.model.max_age_in_hours))