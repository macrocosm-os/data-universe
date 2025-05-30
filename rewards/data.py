import json
from typing import Dict, Optional, List, Tuple
from pydantic import BaseModel, ConfigDict, Field, PositiveInt, field_validator, model_validator
from datetime import datetime
from common.data import DataLabel, DataSource, StrictBaseModel
from common import utils


class Job(StrictBaseModel):
    """Information associated with one incentivized submission in Dynamic Desirability"""
    model_config = ConfigDict(frozen=True)

    keyword: Optional[str] = Field(
        description="Optionally, the keyword associated with the job."
    )

    label: str = Field(
        description="The actual incentivized label or keyword."
    )

    job_weight: float = Field(
        ge=-1,
        le=5,
        description="The reward for data associated with the job."
    )

    # Store as time bucket IDs instead of datetime strings
    start_timebucket: Optional[int] = Field(
        default=None,
        description="Optionally, the earliest viable time bucket at which data is accepted."
    )

    end_timebucket: Optional[int] = Field(
        default=None,
        description="Optionally, the latest viable time bucket at which data is accepted."
    )

    def matches(self, data_keyword: str, data_label: str, data_timebucket: int) -> bool:
        """Check if the incoming data matches this job's criteria using time buckets directly."""
        # First check keyword and label match
        if data_keyword != self.keyword or data_label != self.label:
            return False
            
        # If we have time bucket constraints, check them
        if self.start_timebucket and data_timebucket < self.start_timebucket:
            return False  # Data is before job's start time
                
        if self.end_timebucket and data_timebucket > self.end_timebucket:
            return False  # Data is after job's end time
            
        # All criteria passed
        return True
   
    def __str__(self) -> str:
        return (
            f"Job(keyword={self.keyword!r}, label={self.label!r}, weight={self.job_weight}, "
            f"start={self.start_timebucket}, end={self.end_timebucket})"
        )

    def __repr__(self) -> str:
        return self.__str__()
    
    def to_primitive(self) -> dict:
        """Convert to primitive dictionary representation"""
        return {
            "keyword": self.keyword,
            "label": self.label,
            "job_weight": self.job_weight,
            "start_timebucket": self.start_timebucket,
            "end_timebucket": self.end_timebucket
        }


class JobMatcher(StrictBaseModel):
    """A utility class for efficiently matching jobs to incoming data"""
    
    jobs: List[Job] = Field(default_factory=list)
    job_dict: Dict[Tuple[str, str], List[Job]] = Field(
        default_factory=dict, 
        exclude=True  # Don't include in serialization
    )
    
    class Config:
        arbitrary_types_allowed = True
    
    def __init__(self, **data):
        super().__init__(**data)
        self._build_job_dict()
    
    def _build_job_dict(self):
        """Build the job dictionary for efficient lookups"""
        self.job_dict.clear()
        for job in self.jobs:
            key = (job.keyword, job.label)
            if key not in self.job_dict:
                self.job_dict[key] = []
            self.job_dict[key].append(job)
    
    def find_matching_jobs(self, data_keyword: str, data_label: str, 
                           data_timebucket: int) -> List[Job]:
        """Find all jobs matching the given criteria using time buckets directly
        
        Args:
            data_keyword: The optional keyword of the incoming data
            data_label: The label of the incoming data
            data_timebucket: The time bucket ID of the data
            
        Returns:
            List of matching Job objects
        """
        key = (data_keyword, data_label)
        
        # No jobs match this keyword and label
        if key not in self.job_dict:
            return []
            
        matching_jobs = []
        
        # Check each potential job for time bucket constraints
        for job in self.job_dict[key]:
            # Check if job's start time is after data's time bucket (no overlap)
            if job.start_timebucket and data_timebucket < job.start_timebucket:
                continue
                
            # Check if job's end time is before data's time bucket (no overlap)
            if job.end_timebucket and data_timebucket > job.end_timebucket:
                continue
                
            matching_jobs.append(job)
        
        return matching_jobs


class DataSourceDesirability(StrictBaseModel):
    """The Desirability configuration for a data source."""

    model_config = ConfigDict(frozen=True)

    weight: float = Field(
        ge=0,
        le=1,
        description="The percentage of total reward allocated to this data source.",
    )

    default_scale_factor: float = Field(
        ge=-1,
        le=1,
        default=0.3,
        description="The scaling factor used for data that doesn't match any jobs.",
    )

    job_matcher: JobMatcher = Field(
        default_factory=JobMatcher,
        description="The JobMatcher used to find matching jobs for incoming data.",
    )

    def model_dump_json(self, **kwargs):
        """Custom JSON serialization"""
        jobs_data = []
        for job in self.job_matcher.jobs:
            job_dict = {}
            primitive = job.to_primitive()
            for key, value in primitive.items():
                # Exclude None params to keep logs concise
                if value is not None:
                    job_dict[key] = value
            jobs_data.append(job_dict)
        
        result = {
            "weight": self.weight,
            "default_scale_factor": self.default_scale_factor,
            "jobs": jobs_data
        }
        
        return result

    def __str__(self) -> str:
        return json.dumps(self.model_dump_json(), indent=4)
    
    def to_primitive_data_source_desirability(self) -> "PrimitiveDataSourceDesirability":
        """Convert to a primitive version for performance"""
        return PrimitiveDataSourceDesirability(
            weight=self.weight,
            default_scale_factor=self.default_scale_factor,
            jobs=[job.to_primitive() for job in self.job_matcher.jobs]
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
        distribution_dict = {}
        for key, desirability in self.distribution.items():
            try:
                data_source = DataSource(key)  # Convert int to enum
                key_name = data_source.name
            except ValueError:
                key_name = f"UNKNOWN_{key}"
            
            distribution_dict[key_name] = desirability.model_dump_json()
        
        return json.dumps({
            "distribution": distribution_dict,
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
                data_source_desirability.weight
                for data_source_desirability in distribution.values()
            )
            != 1.0
        ):
            raise ValueError("The data source weights must sum to 1.0")
        return distribution
    
    def to_primitive_data_desirability_lookup(self) -> "PrimitiveDataDesirabilityLookup":
        """Convert to a performance-optimized primitive version."""
        return PrimitiveDataDesirabilityLookup(
            distribution={
                data_source: desirability.to_primitive_data_source_desirability() 
                for data_source, desirability in self.distribution.items()
            },
            max_age_in_hours=self.max_age_in_hours
        )


## Primitive Versions ##

class PrimitiveDataSourceDesirability:
    """A performance-optimized version of DataSourceDesirability."""
    
    def __init__(
        self,
        weight: float,
        default_scale_factor: float,
        jobs: List[dict],
    ):
        self.weight = weight
        self.default_scale_factor = default_scale_factor
        self.jobs = jobs
        
        # Process jobs to convert datetime strings to time bucket IDs
        for job in self.jobs:
            # Convert datetime strings if they haven't been converted yet
            if "start_timebucket" in job and job["start_timebucket"] and "start_timebucket" not in job:
                start_dt = datetime.fromisoformat(job["start_timebucket"])
                job["start_timebucket"] = utils.time_bucket_id_from_datetime(start_dt)
                
            if "end_timebucket" in job and job["end_timebucket"] and "end_timebucket" not in job:
                end_dt = datetime.fromisoformat(job["end_timebucket"])
                job["end_timebucket"] = utils.time_bucket_id_from_datetime(end_dt)
        
        # Build lookup structure
        self._job_dict = {}
        for job in jobs:
            key = (job["keyword"], job["label"])  
            if key not in self._job_dict:
                self._job_dict[key] = []
            self._job_dict[key].append(job)
    
    def find_matching_jobs(self, data_keyword: str, data_label: str, data_timebucket: int) -> List[dict]:
        """Find matching jobs using the optimized lookup structure with time buckets.
        
        Args:
            data_keyword: The keyword of the incoming data
            data_label: The label of the incoming data
            data_timebucket: The time bucket ID of the data
            
        Returns:
            List of matching job dictionaries
        """
        key = (data_keyword, data_label)
        
        # No jobs match this keyword and label
        if key not in self._job_dict:
            return []
        
        matching_jobs = []
        
        # Check each potential job for time bucket constraints
        for job in self._job_dict[key]:
            # Check start time constraint
            if "start_timebucket" in job and job["start_timebucket"] is not None:
                if data_timebucket < job["start_timebucket"]:
                    continue  # Data is before job's start time
                    
            # Check end time constraint
            if "end_timebucket" in job and job["end_timebucket"] is not None:
                if data_timebucket > job["end_timebucket"]:
                    continue  # Data is after job's end time

            matching_jobs.append(job)
        
        return matching_jobs


class PrimitiveDataDesirabilityLookup:
    """A performance-optimized version of DataDesirabilityLookup."""
    
    def __init__(
        self,
        distribution: Dict[DataSource, PrimitiveDataSourceDesirability],
        max_age_in_hours: int
    ):
        self.distribution = distribution
        self.max_age_in_hours = max_age_in_hours
    
    def find_matching_jobs(self, data_source: DataSource, keyword: str, 
                          label: str, data_timebucket: int) -> List[Dict]:
        """Find matching jobs for a specific data source and criteria using time buckets.
        
        Args:
            data_source: The data source to look for
            keyword: The keyword to match
            label: The label to match
            data_timebucket: The time bucket ID
            
        Returns:
            List of matching job dictionaries
        """
        if data_source not in self.distribution:
            return []
        
        return self.distribution[data_source].find_matching_jobs(
            keyword, label, data_timebucket
        )
    
    def get_default_scale_factor(self, data_source: DataSource) -> float:
        """Get the default scale factor for a data source."""
        if data_source not in self.distribution:
            return 0.0  # No reward for unknown data sources
        
        return self.distribution[data_source].default_scale_factor
    
    def get_data_source_weight(self, data_source: DataSource) -> float:
        """Get the weight for a data source."""
        if data_source not in self.distribution:
            return 0.0  # No weight for unknown data sources
        
        return self.distribution[data_source].weight