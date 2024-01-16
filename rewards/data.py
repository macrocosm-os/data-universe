from typing import Dict
from pydantic import field_validator, BaseModel, ConfigDict, Field, PositiveInt

from common.data import DataLabel, DataSource, StrictBaseModel


class DataSourceDesirability(StrictBaseModel):
    """The Desirability for a data source."""

    # Makes the object "Immutable" once created.
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
        description="The scaling factor used for each Label. If a Label is not present, the default_scale_factor is used. The values must be between -1 and 1, inclusive.",
        default_factory=lambda: {},
    )

    @field_validator("label_scale_factors")
    @classmethod
    def validate_label_scale_factors(
        cls, value: Dict[DataLabel, float]
    ) -> Dict[str, float]:
        """Validates the label_scale_factors field."""
        for label, scale_factor in value.items():
            if scale_factor < -1.0 or scale_factor > 1.0:
                raise ValueError(
                    f"Label {label} scale factors must be between -1 and 1, inclusive."
                )
        return value


class DataDesirabilityLookup(StrictBaseModel):
    """Information about data desirability across data sources."""

    # Makes the object "Immutable" once created.
    model_config = ConfigDict(frozen=True)

    distribution: Dict[DataSource, DataSourceDesirability] = Field(
        description="The Desirability for each data source. All data sources must be present and the sum of weights must equal 1.0."
    )

    max_age_in_hours: PositiveInt = Field(
        description="The maximum age of data that will receive rewards. Data older than this will score 0",
    )

    @field_validator("distribution")
    @classmethod
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
