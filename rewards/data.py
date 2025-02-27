import json
from typing import Dict, List, Any
from pydantic import BaseModel, ConfigDict, Field, PositiveInt, field_validator

from common.data import DataLabel, DataSource, StrictBaseModel, DateRange


class DynamicSourceDesirability(StrictBaseModel):
    """The Dynamic Source Desirability for a data source with time-specific settings."""

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

    label_config: Dict[DataLabel, Dict[str, List[DateRange]]] = Field(
        description="The scaling factor used for each Label with time ranges. Contains 'label_weight' (float) and 'date_ranges' (list of DateRange objects).",
        default_factory=lambda: {},
    )

    def model_dump_json(self, **kwargs):
        """Custom JSON serialization"""
        label_factors = {}
        for label, config in self.label_config.items():
            date_ranges_json = [
                {"start": dr.start.isoformat(), "end": dr.end.isoformat()} 
                for dr in config.get("date_ranges", [])
            ]
            label_factors[label.value.replace('"', '')] = {
                "label_weight": config.get("label_weight", 0.0),
                "date_ranges": date_ranges_json
            }
            
        return {
            "weight": self.weight,
            "default_scale_factor": self.default_scale_factor,
            "label_config": label_factors
        }

    def __str__(self) -> str:
        return json.dumps(self.model_dump_json(), indent=4)

    @field_validator("label_config")
    def validate_label_config(
        cls, value: Dict[DataLabel, Dict[str, Any]]
    ) -> Dict[DataLabel, Dict[str, Any]]:
        """Validates the label_config field."""
        for label, config in value.items():
            # Validate weight
            label_weight = config.get("label_weight")
            if label_weight is None:
                raise ValueError(f"Label {label} must have a label_weight defined")
                
            if label_weight < -1.0 or label_weight > 251:
                raise ValueError(
                    f"Label {label} label_weight must be between -1 and 251, inclusive."
                )
                
            # Validate date_ranges
            date_ranges = config.get("date_ranges")
            if date_ranges is None:
                raise ValueError(f"Label {label} must have date_ranges defined")
                
            if not isinstance(date_ranges, list):
                raise ValueError(f"Label {label} date_ranges must be a list")
                
            for dr in date_ranges:
                if not isinstance(dr, DateRange):
                    raise ValueError(f"All items in Label {label} date_ranges must be DateRange objects")
                
                if dr.end <= dr.start:
                    raise ValueError(f"DateRange end must be after start for Label {label}")
                    
        return value

    def get_label_weight(self, label: DataLabel) -> float:
        """
        Gets the weight for a specific label, or returns the default_scale_factor if 
        the label is not explicitly set in label_config.
        
        Args:
            label: The DataLabel to get the weight for
            
        Returns:
            The label_weight if the label is in label_config, otherwise the default_scale_factor
        """
        label_config = self.label_config.get(label)
        if label_config is not None:
            return label_config.get("label_weight", self.default_scale_factor)
        return self.default_scale_factor

    @classmethod
    def to_primitive_dynamic_source_desirability(
        cls, obj: "DynamicSourceDesirability"
    ) -> "PrimitiveDynamicSourceDesirability":
        primitive_label_factors = {}
        
        for label, config in obj.label_config.items():
            primitive_label_factors[label.value if label else None] = {
                "label_weight": config.get("label_weight", 0.0),
                "date_ranges": [
                    {"start": dr.start.isoformat(), "end": dr.end.isoformat()}
                    for dr in config.get("date_ranges", [])
                ]
            }
            
        return PrimitiveDynamicSourceDesirability(
            weight=obj.weight,
            default_scale_factor=obj.default_scale_factor,
            label_config=primitive_label_factors,
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
                DataSource(data_source).name: desirability.model_dump_json()  # Use .name instead of str()
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
    def to_primitive_data_desirability_lookup(
        cls, obj: "DynamicDesirabilityLookup"
    ) -> "PrimitiveDynamicDesirabilityLookup":
        return PrimitiveDynamicDesirabilityLookup(
            distribution={
                data_source: DynamicSourceDesirability.to_primitive_data_source_desirability(
                    data_source_reward
                )
                for data_source, data_source_reward in obj.distribution.items()
            },
            max_age_in_hours=obj.max_age_in_hours,
        )
    

class PrimitiveDynamicSourceDesirability(StrictBaseModel):
    """The Dynamic Desirability for a data source with time-specific settings, using primitive objects for performance"""

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

    label_config: Dict[str, Dict[str, Any]] = Field(
        description="The scaling factor used for each Label with time ranges. Contains 'label_weight' (float) and 'date_ranges' (list of dictionaries with 'start' and 'end' as ISO datetime strings).",
        default_factory=lambda: {},
    )


class PrimitiveDynamicDesirabilityLookup(StrictBaseModel):
    """A DynamicDesirabilityLookup using primitives, for performance."""

    model_config = ConfigDict(frozen=True)

    distribution: Dict[DataSource, PrimitiveDynamicSourceDesirability] = Field(
        description="The Desirability for each data source. All data sources must be present and the sum of weights must equal 1.0."
    )

    max_age_in_hours: PositiveInt = Field(
        description="The maximum age of data that will receive rewards. Data older than this without a specified date range will score 0",
    )


# class DataSourceDesirability(StrictBaseModel):
#     """The Desirability for a data source."""

#     model_config = ConfigDict(frozen=True)

#     weight: float = Field(
#         ge=0,
#         le=1,
#         description="The percentage of total reward that is allocated to this data source.",
#     )

#     default_scale_factor: float = Field(
#         ge=-1,
#         le=1,
#         default=1.0,
#         description="The scaling factor used for all Labels that aren't explicitly set in label_scale_factors.",
#     )

#     label_scale_factors: Dict[DataLabel, float] = Field(
#         description="The scaling factor used for each Label. If a Label is not present, the default_scale_factor is used. The values must be between -1 and 23.33, inclusive.",
#         default_factory=lambda: {},
#     )

#     def model_dump_json(self, **kwargs):
#         """Custom JSON serialization"""
#         return {
#             "weight": self.weight,
#             "default_scale_factor": self.default_scale_factor,
#             "label_scale_factors": {
#                 label.value.replace('"', ''): scale_factor
#                 for label, scale_factor in self.label_scale_factors.items()
#             }
#         }

#     def __str__(self) -> str:
#         return json.dumps(self.model_dump_json(), indent=4)

#     @field_validator("label_scale_factors")
#     def validate_label_scale_factors(
#         cls, value: Dict[DataLabel, float]
#     ) -> Dict[str, float]:
#         """Validates the label_scale_factors field."""
#         for label, scale_factor in value.items():
#             # Max label weight for one active validator putting 100% on one label = 250 + 1 from default.json if applicable
#             if scale_factor < -1.0 or scale_factor > 251:
#                 raise ValueError(
#                     f"Label {label} scale factors must be between -1 and 251, inclusive."
#                 )
#         return value

#     @classmethod
#     def to_primitive_data_source_desirability(
#         cls, obj: "DataSourceDesirability"
#     ) -> "PrimitiveDataSourceDesirability":
#         return PrimitiveDataSourceDesirability(
#             weight=obj.weight,
#             default_scale_factor=obj.default_scale_factor,
#             label_scale_factors={
#                 label.value if label else None: scale_factor
#                 for label, scale_factor in obj.label_scale_factors.items()
#             },
#         )


# class DataDesirabilityLookup(StrictBaseModel):
#     """Information about data desirability across data sources."""

#     model_config = ConfigDict(frozen=True)

#     distribution: Dict[DataSource, DataSourceDesirability] = Field(
#         description="The Desirability for each data source. All data sources must be present and the sum of weights must equal 1.0."
#     )

#     max_age_in_hours: PositiveInt = Field(
#         description="The maximum age of data that will receive rewards. Data older than this will score 0",
#     )

#     def __str__(self) -> str:
#         return json.dumps({
#             "distribution": {
#                 DataSource(data_source).name: desirability.model_dump_json()  # Use .name instead of str()
#                 for data_source, desirability in self.distribution.items()
#             },
#             "max_age_in_hours": self.max_age_in_hours
#         }, indent=4)

#     def __repr__(self) -> str:
#         return self.__str__()
    
#     @field_validator("distribution")
#     def validate_distribution(
#         cls, distribution: Dict[DataSource, DataSourceDesirability]
#     ) -> Dict[DataSource, DataSourceDesirability]:
#         """Validates the distribution field."""
#         if (
#             sum(
#                 data_source_reward.weight
#                 for data_source_reward in distribution.values()
#             )
#             != 1.0
#         ):
#             raise ValueError("The data source weights must sum to 1.0")
#         return distribution

#     @classmethod
#     def to_primitive_data_desirability_lookup(
#         cls, obj: "DataDesirabilityLookup"
#     ) -> "PrimitiveDataDesirabilityLookup":
#         return PrimitiveDataDesirabilityLookup(
#             distribution={
#                 data_source: DataSourceDesirability.to_primitive_data_source_desirability(
#                     data_source_reward
#                 )
#                 for data_source, data_source_reward in obj.distribution.items()
#             },
#             max_age_in_hours=obj.max_age_in_hours,
#         )


# class PrimitiveDataSourceDesirability(StrictBaseModel):
#     """The Desirability for a data source, using primitive objects for performance"""

#     model_config = ConfigDict(frozen=True)

#     weight: float = Field(
#         ge=0,
#         le=1,
#         description="The percentage of total reward that is allocated to this data source.",
#     )

#     default_scale_factor: float = Field(
#         ge=-1,
#         le=1,
#         default=1.0,
#         description="The scaling factor used for all Labels that aren't explicitly set in label_scale_factors.",
#     )

#     label_scale_factors: Dict[str, float] = Field(
#         description="The scaling factor used for each Label. If a Label is not present, the default_scale_factor is used. The values must be between -1 and 1, inclusive.",
#         default_factory=lambda: {},
#     )



# class PrimitiveDataDesirabilityLookup(StrictBaseModel):
#     """A DataDesirabilityLookup using primitives, for performance."""

#     model_config = ConfigDict(frozen=True)

#     distribution: Dict[DataSource, PrimitiveDataSourceDesirability] = Field(
#         description="The Desirability for each data source. All data sources must be present and the sum of weights must equal 1.0."
#     )

#     max_age_in_hours: PositiveInt = Field(
#         description="The maximum age of data that will receive rewards. Data older than this will score 0",
#     )
