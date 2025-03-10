import json
from typing import Dict, Tuple, Optional
from pydantic import BaseModel, ConfigDict, Field, PositiveInt, field_validator
import datetime as dt
from common.data import DataLabel, DataSource, StrictBaseModel

class DynamicSourceDesirability(StrictBaseModel):
    """The Dynamic (optionally time-based) Desirability for a data source."""
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
        description="The scaling factor used for all Labels that aren't explicitly set in label_config.",
    )
    
    label_config: Dict[DataLabel, Tuple[float, Optional[dt.datetime]]] = Field(
        description=(
            "The scaling factor and earliest viable datetime used for each DataLabel. "
            "If a Label is not present, the default_scale_factor is used. "
            "The scaling factor values must be between -1 and 251, inclusive."
        ),
        default_factory=lambda: {},
    )
    
    def model_dump_json(self, **kwargs):
        """Custom JSON serialization"""
        return {
            "weight": self.weight,
            "default_scale_factor": self.default_scale_factor,
            "label_config": {
                label.value.replace('"', ''): {
                    "scale_factor": scale_factor,
                    "earliest_viable_datetime": earliest_dt.isoformat() if earliest_dt else None
                }
                for label, (scale_factor, earliest_dt) in self.label_config.items()
            }
        }
        
    def __str__(self) -> str:
        return json.dumps(self.model_dump_json(), indent=4)
    
    @field_validator("label_config")
    def validate_label_config(
        cls, value: Dict[DataLabel, Tuple[float, dt.datetime]]
    ) -> Dict[DataLabel, Tuple[float, dt.datetime]]:
        """Validates the label_config field."""
        current_time = dt.datetime.now(dt.timezone.utc) 
        
        for label, (scale_factor, timestamp) in value.items():
            # Max label weight for one active validator putting 100% on one label = 250 + 1 from default.json if applicable
            if scale_factor < -1.0 or scale_factor > 251:
                raise ValueError(
                    f"Label {label} scale factors must be between -1 and 251, inclusive."
                )
            
            # Check that the timestamp is not in the future
            if timestamp:
                # Converts timestamp to UTC if it has timezone info, or add UTC if it doesn't
                ts_utc = timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=dt.timezone.utc)
                if ts_utc > current_time:
                    raise ValueError(
                        f"Label {label} has a timestamp in the future ({timestamp}), which is not allowed."
                    )
                
        return value
    
    @classmethod
    def to_primitive_dynamic_source_desirability(
        cls, obj: "DynamicSourceDesirability"
    ) -> "PrimitiveDynamicSourceDesirability":
        return PrimitiveDynamicSourceDesirability(
            weight=obj.weight,
            default_scale_factor=obj.default_scale_factor,
            label_config={
                label.value.replace('"', '').lower(): (scale_factor, earliest_dt.isoformat() if earliest_dt else "")
                for label, (scale_factor, earliest_dt) in obj.label_config.items()
            },
        )
    
class PrimitiveDynamicSourceDesirability(StrictBaseModel):
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
        description="The scaling factor used for all Labels that aren't explicitly set in label_config.",
    )

    label_config: Dict[str, Tuple[float, str]] = Field(
        description=(
            "The scaling factor and earliest viable datetime used for each DataLabel. "
            "If a Label is not present, the default_scale_factor is used. "
            "The scaling factor values must be between -1 and 251, inclusive."
        ),
        default_factory=lambda: {},
    )


class DynamicDesirabilityLookup(StrictBaseModel):
    """Information about data desirability across data sources."""

    model_config = ConfigDict(frozen=True)

    distribution: Dict[DataSource, DynamicSourceDesirability] = Field(
        description="The Desirability for each data source. All data sources must be present and the sum of weights must equal 1.0."
    )

    max_age_in_hours: PositiveInt = Field(
        description="The maximum age of data that will receive rewards. Data older than this will score 0",
    )

    def __str__(self) -> str:
        return json.dumps({
            "distribution": {
                DataSource(data_source).name: desirability.model_dump_json() 
                for data_source, desirability in self.distribution.items()
            },
            "max_age_in_hours": self.max_age_in_hours
        }, indent=4)

    def __repr__(self) -> str:
        return self.__str__()
    
    @field_validator("distribution")
    def validate_distribution(
        cls, distribution: Dict[DataSource, DynamicSourceDesirability]
    ) -> Dict[DataSource, DynamicSourceDesirability]:
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
    def to_primitive_dynamic_desirability_lookup(
        cls, obj: "DynamicDesirabilityLookup"
    ) -> "PrimitiveDynamicDesirabilityLookup":
        return PrimitiveDynamicDesirabilityLookup(
            distribution={
                data_source: DynamicSourceDesirability.to_primitive_dynamic_source_desirability(
                    data_source_reward
                )
                for data_source, data_source_reward in obj.distribution.items()
            },
            max_age_in_hours=obj.max_age_in_hours,
        )


class PrimitiveDynamicDesirabilityLookup(StrictBaseModel):
    """A DynamicDesirabilityLookup using primitives, for performance."""

    model_config = ConfigDict(frozen=True)

    distribution: Dict[DataSource, PrimitiveDynamicSourceDesirability] = Field(
        description="The Desirability for each data source. All data sources must be present and the sum of weights must equal 1.0."
    )

    max_age_in_hours: PositiveInt = Field(
        description="The maximum age of data that will receive rewards. Data older than this will score 0",
    )