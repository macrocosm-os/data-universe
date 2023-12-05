from typing import Dict
from pydantic import BaseModel, ConfigDict, Field, field_validator

from common.data import DataSource


class DataSourceReward(BaseModel):
    """The reward for a data source."""

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

    label_scale_factors: Dict[str, float] = Field(
        description="The scaling factor used for each Label. If a Label is not present, the default_scale_factor is used. The values must be between -1 and 1, inclusive.",
        default_factory=lambda: {},
    )

    @field_validator("label_scale_factors")
    @classmethod
    def validate_label_scale_factors(cls, value: Dict[str, float]) -> Dict[str, float]:
        """Validates the label_scale_factors field."""
        for label, scale_factor in value.items():
            if scale_factor < 1.0 or scale_factor > 1.0:
                raise ValueError(
                    f"Label {label} scale factors must be between -1 and 1, inclusive."
                )
        return value


class RewardDistributionData(BaseModel):
    """The data model for how rewards are distributed across data sources."""

    # Makes the object "Immutable" once created.
    model_config = ConfigDict(frozen=True)

    distribution: Dict[DataSource, DataSourceReward] = Field(
        description="The reward distribution for each data source. All data sources must be present and the sum of weights must equal 1.0."
    )

    max_age_in_hours = Field(
        ge=0,
        description="The maximum age of data that will receive rewards. Data older than this will score 0",
    )

    @field_validator("distribution")
    @classmethod
    def validate_distribution(
        cls, distribution: Dict[DataSource, DataSourceReward]
    ) -> Dict[DataSource, DataSourceReward]:
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
