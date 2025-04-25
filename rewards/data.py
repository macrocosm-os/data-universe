import json
from typing import Dict, Optional, List
from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field, PositiveInt, field_validator, model_validator
from common.data import DataLabel, DataSource, StrictBaseModel


class JobParams(StrictBaseModel):
    """Parameters for a job in the new schema format."""
    
    job_type: str = Field(
        description="Type of job, either 'keyword' or 'label'",
    )

    platform: str = Field(
        description="Platform for the job, either 'reddit' or 'x'",
    )

    topic: str = Field(
        description="Topic or label for the job",
    )

    post_start_datetime: Optional[str] = Field(
        default=None, 
        description="Earliest datetime in UTC that a post will receive score from",
    )

    post_end_datetime: Optional[str] = Field(
        default=None, 
        description="Latest datetime in UTC that a post will receive score from",
    )
    
    @field_validator("post_start_datetime", "post_end_datetime")
    def validate_datetimes(cls, value):
        """Validate datetime strings are in correct format if provided."""
        if value is not None:
            try:
                datetime.fromisoformat(value.replace('Z', '+00:00'))
            except ValueError:
                raise ValueError(f"Invalid datetime format: {value}. Expected ISO format.")
        return value
    
    @field_validator("post_start_datetime", "post_end_datetime")
    def validate_datetime_order(cls, value, info):
        """Validate post_start_datetime is before post_end_datetime."""
        if info.data.get("post_start_datetime") and info.data.get("post_end_datetime"):
            start = datetime.fromisoformat(info.data["post_start_datetime"].replace('Z', '+00:00'))
            end = datetime.fromisoformat(info.data["post_end_datetime"].replace('Z', '+00:00'))
            if start >= end:
                raise ValueError("post_start_datetime must be before post_end_datetime")
        return value
    

class Job(StrictBaseModel):
    """Represents a job in the new schema format."""
    
    id: str = Field(
        description="Unique identifier for the job",
    )
    weight: float = Field(
        ge=0, 
        le=5,
        description="Weight for the job", 
    )
    params: JobParams = Field(
        description="Parameters for the job, including data platform, time range, etc.",
    )


class JobLookup(StrictBaseModel):
    """Information about dynamic desirabilities across all Jobs."""

    model_config = ConfigDict(frozen=True)

    job_list: List[Job] = Field(
        description="The list of all active jobs in Dynamic Desirability.",
    )

    platform_weights: Dict[str, float] = Field(
        description="The platform names and associated platform weights. Weights must sum to 1 across all platforms.",
    )

    default_label_weight: float = Field(
        default= 0.3,
        description="The default weight for 'label' job types (subreddit, hashtag).",
    )

    default_keyword_weight: float = Field(
        default=0.0,            # increased after keyword scoring
        description="The default weight for 'keyword' job types.",
    )

    max_age_in_hours: PositiveInt = Field(
        description="The maximum age of data that will receive rewards for jobs without a specified time range. Data older than this will score 0.",
    )

    @field_validator("platform_weights")
    def validate_platform_weights(
        cls, platform_weights: Dict[str, float]
    ) -> Dict[str, float]:
        """Validates that the platform weights sum to 1.0 exactly."""

        if sum(platform_weights.values()) != 1.0:
            raise ValueError("The platform weights must sum to 1.0")
        return platform_weights
    
    @model_validator(mode="after")
    def validate_platform_coverage(self) -> "JobLookup":
        job_platforms = {job.params.platform for job in self.job_list}
        weight_platforms = set(self.platform_weights.keys())
        if not job_platforms.issubset(weight_platforms):
            raise ValueError(f"All job platforms {job_platforms} must exist in platform_weights {weight_platforms}")
        return self



class DataSourceDesirability(StrictBaseModel):
    """The Desirability for a data source."""

    model_config = ConfigDict(frozen=True)

    weight: float = Field(
        ge=0,
        le=1,
        description="The percentage of total reward that is allocated to this data source.",
    )

    default_scale_factor: float = Field(
        ge=-1,
        le=1,
        default=1.0,
        description="The scaling factor used for all Labels that aren't explicitly set in label_scale_factors.",
    )

    label_scale_factors: Dict[DataLabel, float] = Field(
        description="The scaling factor used for each Label. If a Label is not present, the default_scale_factor is used. The values must be between -1 and 23.33, inclusive.",
        default_factory=lambda: {},
    )

    def model_dump_json(self, **kwargs):
        """Custom JSON serialization"""
        return {
            "weight": self.weight,
            "default_scale_factor": self.default_scale_factor,
            "label_scale_factors": {
                label.value.replace('"', ''): scale_factor
                for label, scale_factor in self.label_scale_factors.items()
            }
        }

    def __str__(self) -> str:
        return json.dumps(self.model_dump_json(), indent=4)

    @field_validator("label_scale_factors")
    def validate_label_scale_factors(
        cls, value: Dict[DataLabel, float]
    ) -> Dict[str, float]:
        """Validates the label_scale_factors field."""
        for label, scale_factor in value.items():
            # Max label weight for one active validator putting 100% on one label = 250 + 1 from default.json if applicable
            if scale_factor < -1.0 or scale_factor > 251:
                raise ValueError(
                    f"Label {label} scale factors must be between -1 and 251, inclusive."
                )
        return value

    @classmethod
    def to_primitive_data_source_desirability(
        cls, obj: "DataSourceDesirability"
    ) -> "PrimitiveDataSourceDesirability":
        return PrimitiveDataSourceDesirability(
            weight=obj.weight,
            default_scale_factor=obj.default_scale_factor,
            label_scale_factors={
                label.value if label else None: scale_factor
                for label, scale_factor in obj.label_scale_factors.items()
            },
        )


class DataDesirabilityLookup(StrictBaseModel):
    """Information about data desirability across data sources."""

    model_config = ConfigDict(frozen=True)

    distribution: Dict[DataSource, DataSourceDesirability] = Field(
        description="The Desirability for each data source. All data sources must be present and the sum of weights must equal 1.0."
    )

    max_age_in_hours: PositiveInt = Field(
        description="The maximum age of data that will receive rewards. Data older than this will score 0",
    )

    def __str__(self) -> str:
        return json.dumps({
            "distribution": {
                DataSource(data_source).name: desirability.model_dump_json()  # Use .name instead of str()
                for data_source, desirability in self.distribution.items()
            },
            "max_age_in_hours": self.max_age_in_hours
        }, indent=4)

    def __repr__(self) -> str:
        return self.__str__()
    
    @field_validator("distribution")
    def validate_distribution(
        cls, distribution: Dict[DataSource, DataSourceDesirability]
    ) -> Dict[DataSource, DataSourceDesirability]:
        """Validates the distribution field."""
        if (
            sum(
                data_source_reward.weight
                for data_source_reward in distribution.values()
            )
            != 1.0
        ):
            raise ValueError("The data source weights must sum to 1.0")
        return distribution

    @classmethod
    def to_primitive_data_desirability_lookup(
        cls, obj: "DataDesirabilityLookup"
    ) -> "PrimitiveDataDesirabilityLookup":
        return PrimitiveDataDesirabilityLookup(
            distribution={
                data_source: DataSourceDesirability.to_primitive_data_source_desirability(
                    data_source_reward
                )
                for data_source, data_source_reward in obj.distribution.items()
            },
            max_age_in_hours=obj.max_age_in_hours,
        )


class PrimitiveDataSourceDesirability(StrictBaseModel):
    """The Desirability for a data source, using primitive objects for performance"""

    model_config = ConfigDict(frozen=True)

    weight: float = Field(
        ge=0,
        le=1,
        description="The percentage of total reward that is allocated to this data source.",
    )

    default_scale_factor: float = Field(
        ge=-1,
        le=1,
        default=1.0,
        description="The scaling factor used for all Labels that aren't explicitly set in label_scale_factors.",
    )

    label_scale_factors: Dict[str, float] = Field(
        description="The scaling factor used for each Label. If a Label is not present, the default_scale_factor is used. The values must be between -1 and 1, inclusive.",
        default_factory=lambda: {},
    )


class PrimitiveDataDesirabilityLookup(StrictBaseModel):
    """A DataDesirabilityLookup using primitives, for performance."""

    model_config = ConfigDict(frozen=True)

    distribution: Dict[DataSource, PrimitiveDataSourceDesirability] = Field(
        description="The Desirability for each data source. All data sources must be present and the sum of weights must equal 1.0."
    )

    max_age_in_hours: PositiveInt = Field(
        description="The maximum age of data that will receive rewards. Data older than this will score 0",
    )