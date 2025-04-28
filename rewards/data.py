import json
from typing import Dict, Optional, List, Tuple
from pydantic import BaseModel, ConfigDict, Field, PositiveInt, field_validator, model_validator
from datetime import datetime
from common.data import DataLabel, DataSource, StrictBaseModel


class Job(StrictBaseModel):
    """Information associated with one incentivized submission in Dynamic Desirability"""
    model_config = ConfigDict(frozen=True)

    job_type: str = Field(
        description="The type of job, either 'label' or 'keyword'."
    )

    topic: str = Field(
        description="The actual incentivized label or keyword."
    )

    job_weight: float = Field(
        ge=-1,
        le=5,
        description="The reward for data associated with the job."
    )

    start_datetime: Optional[str] = Field(
        default=None,
        description="Optionally, the earliest viable datetime at which data is accepted."
    )

    end_datetime: Optional[str] = Field(
        default=None,
        description="Optionally, the latest viable datetime at which data is accepted."
    )

    @field_validator("job_type")
    def validate_job_type(cls, v):
        if v not in {"label", "keyword"}:
            raise ValueError("job_type must be either 'label' or 'keyword'")
        return v

    @model_validator(mode='after')
    def check_date_order(self) -> 'Job':
        if self.start_datetime and self.end_datetime:
            try:
                start = datetime.fromisoformat(self.start_datetime)
                end = datetime.fromisoformat(self.end_datetime)
                if start > end:
                    raise ValueError("start_datetime must be before or equal to end_datetime.")
            except ValueError as e:
                raise ValueError(f"Invalid date format or order: {e}")
        return self
    
    def matches(self, data_job_type: str, data_topic: str, data_daterange: Tuple[str, str]) -> bool:
        """Check if the incoming data matches this job's criteria.
        
        Args:
            data_job_type: The job type of the incoming data
            data_topic: The topic of the incoming data
            data_daterange: A tuple of (start_datetime, end_datetime) for the data's date range
        
        Returns:
            True if there's any overlap between the job's date range and the data's date range
        """
        # First check job_type and topic match
        if data_job_type != self.job_type or data_topic != self.topic:
            return False
            
        # If we have date constraints, check them
        data_start, data_end = data_daterange
        data_start_dt = datetime.fromisoformat(data_start)
        data_end_dt = datetime.fromisoformat(data_end)
        
        # If job has start date constraint, check if data's end is before job's start
        if self.start_datetime:
            job_start_dt = datetime.fromisoformat(self.start_datetime)
            if data_end_dt < job_start_dt:
                return False  # Data ends before job starts, no overlap
                
        # If job has end date constraint, check if data's start is after job's end
        if self.end_datetime:
            job_end_dt = datetime.fromisoformat(self.end_datetime)
            if data_start_dt > job_end_dt:
                return False  # Data starts after job ends, no overlap
            
        # All criteria passed - there is an overlap or no date constraints
        return True
    
    def __str__(self) -> str:
        return (
            f"Job(type={self.job_type!r}, topic={self.topic!r}, weight={self.job_weight}, "
            f"start={self.start_datetime}, end={self.end_datetime})"
        )

    def __repr__(self) -> str:
        return self.__str__()
    
    def to_primitive(self) -> dict:
        """Convert to primitive dictionary representation"""
        return {
            "job_type": self.job_type,
            "topic": self.topic,
            "job_weight": self.job_weight,
            "start_datetime": self.start_datetime,
            "end_datetime": self.end_datetime
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
            key = (job.job_type, job.topic)
            if key not in self.job_dict:
                self.job_dict[key] = []
            self.job_dict[key].append(job)
    
    def find_matching_jobs(self, data_job_type: str, data_topic: str, 
                           data_daterange: Tuple[str, str]) -> List[Job]:
        """Find all jobs matching the given criteria
        
        Args:
            data_job_type: The job type of the incoming data
            data_topic: The topic of the incoming data
            data_daterange: A tuple of (start_datetime, end_datetime) for the data's date range
            
        Returns:
            List of matching Job objects
        """
        key = (data_job_type, data_topic)
        
        # No jobs match this job_type and topic
        if key not in self.job_dict:
            return []
            
        data_start, data_end = data_daterange
        data_start_dt = datetime.fromisoformat(data_start)
        data_end_dt = datetime.fromisoformat(data_end)
        matching_jobs = []
        
        # Check each potential job for date viability
        for job in self.job_dict[key]:
            # If job has start date constraint, check if data's end is before job's start
            if job.start_datetime and data_end_dt < datetime.fromisoformat(job.start_datetime):
                continue  # No overlap
                
            # If job has end date constraint, check if data's start is after job's end
            if job.end_datetime and data_start_dt > datetime.fromisoformat(job.end_datetime):
                continue  # No overlap
                
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
        return {
            "weight": self.weight,
            "default_scale_factor": self.default_scale_factor,
            "jobs": [job.to_primitive() for job in self.job_matcher.jobs]
        }

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
        return json.dumps({
            "distribution": {
                data_source.name: desirability.model_dump_json()
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
        
        # Build lookup structure
        self._job_dict = {}
        for job in jobs:
            key = (job["job_type"], job["topic"])
            if key not in self._job_dict:
                self._job_dict[key] = []
            self._job_dict[key].append(job)
    
    def find_matching_jobs(self, data_job_type: str, data_topic: str, data_daterange: Tuple[str, str]) -> List[dict]:
        """Find matching jobs using the optimized lookup structure.
        
        Args:
            data_job_type: The job type of the incoming data
            data_topic: The topic of the incoming data
            data_daterange: A tuple of (start_datetime, end_datetime) for the data's date range
            
        Returns:
            List of matching job dictionaries
        """
        key = (data_job_type, data_topic)
        
        # No jobs match this job_type and topic
        if key not in self._job_dict:
            return []
            
        data_start, data_end = data_daterange
        data_start_dt = datetime.fromisoformat(data_start)
        data_end_dt = datetime.fromisoformat(data_end)
        matching_jobs = []
        
        # Check each potential job for date viability - using direct dictionary access for speed
        for job in self._job_dict[key]:
            # Check if job's start date is after data's end date (no overlap)
            start_datetime = job.get("start_datetime")
            if start_datetime and data_end_dt < datetime.fromisoformat(start_datetime):
                continue
                
            # Check if job's end date is before data's start date (no overlap)
            end_datetime = job.get("end_datetime")
            if end_datetime and data_start_dt > datetime.fromisoformat(end_datetime):
                continue

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
    
    def find_matching_jobs(self, data_source: DataSource, job_type: str, 
                          topic: str, daterange: Tuple[str, str]) -> List[Dict]:
        """Find matching jobs for a specific data source and criteria using date ranges.
        
        Args:
            data_source: The data source to look for
            job_type: The job type to match
            topic: The topic to match
            daterange: A tuple of (start_datetime, end_datetime) strings
            
        Returns:
            List of matching job dictionaries
        """
        if data_source not in self.distribution:
            return []
        
        return self.distribution[data_source].find_matching_jobs(
            job_type, topic, daterange
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