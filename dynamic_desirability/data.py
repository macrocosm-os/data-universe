from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Dict, List, Optional, Union
from datetime import datetime
import bittensor as bt
from decimal import Decimal
from common.constants import MAX_LABEL_LENGTH  
from dynamic_desirability.constants import VALID_SOURCES
import json


class LabelWeights(BaseModel):
    """Model for label weights in the old format"""
    label_weights: Dict[str, float] = Field(description="Mapping of label to weight")
    
    @field_validator('label_weights')
    @classmethod
    def validate_labels_and_weights(cls, label_weights: Dict[str, float]) -> Dict[str, float]:
        # Filter out labels that exceed max length
        filtered_weights = {}
        for label, weight in label_weights.items():
            if len(label) > MAX_LABEL_LENGTH:
                bt.logging.warning(f"Skipping label {label}: exceeds {MAX_LABEL_LENGTH} character limit")
                continue
                
            # Skip non-positive weights
            if weight <= 0:
                bt.logging.warning(f"Skipping label {label} with non-positive weight: {weight}")
                continue
                
            filtered_weights[label] = weight
            
        return filtered_weights


class OldFormatPreference(BaseModel):
    """Model for a single source preference in the old format"""
    source_name: str = Field(description="Platform name (e.g., 'reddit', 'x')")
    label_weights: Dict[str, float] = Field(description="Mapping of label to weight")
    
    @field_validator('source_name')
    @classmethod
    def validate_source(cls, source_name: str) -> str:
        source_lower = source_name.lower()
        if source_lower not in VALID_SOURCES:
            raise ValueError(f"Invalid source: {source_name}. Must be one of {VALID_SOURCES}")
        return source_lower
    
    @field_validator('label_weights')
    @classmethod
    def validate_labels_and_weights(cls, label_weights: Dict[str, float]) -> Dict[str, float]:
        # Filter out labels that exceed max length
        filtered_weights = {}
        for label, weight in label_weights.items():
            if len(label) > MAX_LABEL_LENGTH:
                bt.logging.warning(f"Skipping label {label}: exceeds {MAX_LABEL_LENGTH} character limit")
                continue
                
            # Skip non-positive weights
            if weight <= 0:
                bt.logging.warning(f"Skipping label {label} with non-positive weight: {weight}")
                continue
                
            filtered_weights[label] = weight
            
        return filtered_weights


class JobParams(BaseModel):
    """Model for job parameters in the new format"""
    keyword: Optional[str] = Field(
        None, 
        description="Optional keyword to search for",
        min_length=1,
        max_length=MAX_LABEL_LENGTH
    )
    platform: str = Field(description="Platform to search (e.g., 'reddit', 'x', 'youtube')")
    label: Optional[str] = Field(
        None, 
        description="Optional label or label to search for",
        min_length=1,
        max_length=MAX_LABEL_LENGTH
    )
    post_start_datetime: Optional[str] = Field(None, description="Start date for posts in ISO format")
    post_end_datetime: Optional[str] = Field(None, description="End date for posts in ISO format")
    
    @field_validator('platform')
    @classmethod
    def validate_platform(cls, platform: str) -> str:
        platform_lower = platform.lower()
        if platform_lower not in VALID_SOURCES:
            raise ValueError(f"Invalid platform: {platform}. Must be one of {VALID_SOURCES}")
        return platform_lower
    
    @field_validator('post_start_datetime', 'post_end_datetime')
    @classmethod
    def validate_datetime_format(cls, date_string: Optional[str]) -> Optional[str]:
        if date_string is None:
            return None
            
        try:
            datetime.fromisoformat(date_string)
            return date_string
        except ValueError:
            raise ValueError(f"Invalid datetime format: {date_string}. Must be ISO format")
    
    @model_validator(mode='after')
    def validate_job_params(self) -> 'JobParams':
        # Validate that at least one of keyword or label is provided
        if self.keyword is None and self.label is None:
            raise ValueError("At least one of 'keyword' or 'label' must be provided")
            
        # Validate datetime order if both are present
        start = self.post_start_datetime
        end = self.post_end_datetime
        
        if start and end:
            start_dt = datetime.fromisoformat(start)
            end_dt = datetime.fromisoformat(end)
            
            if start_dt >= end_dt:
                raise ValueError("post_start_datetime must be before post_end_datetime")
                
        return self


class Job(BaseModel):
    """Model for a job in the new format"""
    id: str = Field(description="Unique identifier for the job")
    weight: float = Field(gt=0, description="Weight for the job (positive float)")
    params: JobParams = Field(description="Job parameters")


class PreferencesData(BaseModel):
    """Model for handling either format of preferences data"""
    data: List[Job | OldFormatPreference]
    
    @classmethod
    def parse_and_normalize(cls, data: List[dict], hotkey: Optional[str] = None) -> List[Job]:
        """
        Parse and normalize preferences data to the new job format.
        
        Args:
            data: List of dictionaries in either old or new format
            hotkey: Optional hotkey for generating job IDs when converting old format
            
        Returns:
            List of normalized Job objects in the new format
        """
        # Check format
        is_new_format = all(
            isinstance(item, dict) and 
            "id" in item and 
            "weight" in item and 
            "params" in item and 
            isinstance(item["params"], dict)
            for item in data
        ) if data else False
        
        if is_new_format:
            # Parse as new format
            bt.logging.info("Processing new format job configuration")
            try:
                # This will raise validation errors for invalid jobs
                jobs = [Job.model_validate(job) for job in data]
                return cls._normalize_job_weights(jobs)
            except Exception as e:
                bt.logging.error(f"Error validating jobs: {str(e)}")
                raise
        else:
            # Parse as old format and convert
            bt.logging.info("Processing old format preferences, will convert to new format")
            if not hotkey:
                bt.logging.error("Hotkey required for converting old format to new format")
                raise ValueError("Hotkey required for converting old format to new format")
                
            try:
                # Validate old format
                valid_sources = []
                for source_dict in data:
                    try:
                        valid_source = OldFormatPreference.model_validate(source_dict)
                        if valid_source.label_weights:  # Only include if it has valid labels
                            valid_sources.append(valid_source)
                    except Exception as e:
                        bt.logging.warning(f"Skipping invalid source: {str(e)}")
                
                # Convert to new format
                return cls._convert_to_new_format(valid_sources, hotkey)
            except Exception as e:
                bt.logging.error(f"Error processing old format: {str(e)}")
                raise
    
    @staticmethod
    def _normalize_job_weights(jobs: List[Job]) -> List[Job]:
        """Normalize weights to sum to 1.0"""
        if not jobs:
            bt.logging.error("No valid jobs found after filtering.")
            return []
            
        # Calculate total weight
        total_weight = sum(Decimal(str(job.weight)) for job in jobs)
        
        # Normalize weights
        for job in jobs:
            original_weight = Decimal(str(job.weight))
            job.weight = float(original_weight / total_weight)
        
        return jobs
    
    @staticmethod
    def _convert_to_new_format(sources: List[OldFormatPreference], hotkey: str) -> List[Job]:
        """Convert old format to new format"""
        jobs = []
        job_count = 0
        
        for source in sources:
            for label, weight in source.label_weights.items():
                job_count += 1
                job_id = f"{hotkey}_{job_count}"
                
                jobs.append(Job(
                    id=job_id,
                    weight=float(weight),
                    params=JobParams(
                        keyword=None,
                        platform=source.source_name,
                        label=label,
                        post_start_datetime=None,
                        post_end_datetime=None
                    )
                ))
        
        return PreferencesData._normalize_job_weights(jobs)


def normalize_preferences(data: List[dict], hotkey: Optional[str] = None) -> str:
    """
    Normalize preferences data using Pydantic models and return as JSON string.
    
    Args:
        data: List of dictionaries in either old or new format
        hotkey: Optional hotkey for generating job IDs when converting old format
        
    Returns:
        JSON string of normalized data
    """
    if not data:
        bt.logging.info("Empty desirabilities submitted. Submitting empty vote.")
        return "[]"
        
    try:
        normalized_jobs = PreferencesData.parse_and_normalize(data, hotkey)
        if not normalized_jobs:
            bt.logging.error("No valid jobs after normalization.")
            return None
            
        # Convert to list of dicts for JSON serialization
        return json.dumps([job.model_dump() for job in normalized_jobs], indent=4)
    except Exception as e:
        bt.logging.error(f"Error normalizing preferences: {str(e)}")
        return None