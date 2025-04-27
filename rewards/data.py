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
        ge=0,
        le=5,
        description="The reward for data associated with the job."
    )

    start_date: Optional[str] = Field(
        default=None,
        description="Optionally, the earliest viable date at which data is accepted."
    )

    end_date: Optional[str] = Field(
        default=None,
        description="Optionally, the latest viable date at which data is accepted."
    )

    @field_validator("job_type")
    def validate_job_type(cls, v):
        if v not in {"label", "keyword"}:
            raise ValueError("job_type must be either 'label' or 'keyword'")
        return v

    @model_validator(mode='after')
    def check_date_order(self) -> 'Job':
        if self.start_date and self.end_date:
            try:
                start = datetime.fromisoformat(self.start_date)
                end = datetime.fromisoformat(self.end_date)
                if start > end:
                    raise ValueError("start_date must be before or equal to end_date.")
            except ValueError as e:
                raise ValueError(f"Invalid date format or order: {e}")
        return self
    
    def matches(self, data_job_type: str, data_topic: str, data_datetime: str) -> bool:
        """Check if the incoming data matches this job's criteria."""
        # First check job_type and topic match
        if data_job_type != self.job_type or data_topic != self.topic:
            return False
            
        # If we have date constraints, check them
        dt = datetime.fromisoformat(data_datetime)
        
        # Check start date constraint if it exists
        if self.start_date and dt < datetime.fromisoformat(self.start_date):
            return False
                
        # Check end date constraint if it exists
        if self.end_date and dt > datetime.fromisoformat(self.end_date):
            return False
            
        # All criteria passed
        return True
    
    def __str__(self) -> str:
        return (
            f"Job(type={self.job_type!r}, topic={self.topic!r}, weight={self.job_weight}, "
            f"start={self.start_date}, end={self.end_date})"
        )

    def __repr__(self) -> str:
        return self.__str__()
    
    def to_primitive(self) -> dict:
        """Convert to primitive dictionary representation"""
        return {
            "job_type": self.job_type,
            "topic": self.topic,
            "job_weight": self.job_weight,
            "start_date": self.start_date,
            "end_date": self.end_date
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
                           data_datetime: str) -> List[Job]:
        """Find all jobs matching the given criteria"""
        key = (data_job_type, data_topic)
        
        # No jobs match this job_type and topic
        if key not in self.job_dict:
            return []
            
        dt = datetime.fromisoformat(data_datetime)
        matching_jobs = []
        
        # Check each potential job for date viability
        for job in self.job_dict[key]:
            # Check start date constraint if it exists
            if job.start_date and dt < datetime.fromisoformat(job.start_date):
                continue
                
            # Check end date constraint if it exists
            if job.end_date and dt > datetime.fromisoformat(job.end_date):
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
    
    def find_matching_jobs(self, data_job_type: str, data_topic: str, data_datetime: str) -> List[dict]:
        """Find matching jobs using the optimized lookup structure."""
        key = (data_job_type, data_topic)
        
        # No jobs match this job_type and topic
        if key not in self._job_dict:
            return []
            
        dt = datetime.fromisoformat(data_datetime)
        matching_jobs = []
        
        # Check each potential job for date viability - using direct dictionary access for speed
        for job in self._job_dict[key]:
            # Check start date constraint if it exists
            start_date = job.get("start_date")
            if start_date and dt < datetime.fromisoformat(start_date):
                continue
                
            # Check end date constraint if it exists
            end_date = job.get("end_date")
            if end_date and dt > datetime.fromisoformat(end_date):
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
                           topic: str, datetime_str: str) -> List[Dict]:
        """Find matching jobs for a specific data source and criteria."""
        if data_source not in self.distribution:
            return []
        
        return self.distribution[data_source].find_matching_jobs(
            job_type, topic, datetime_str
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