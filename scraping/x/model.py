import datetime as dt
from typing import List
from pydantic.v1 import BaseModel, ConfigDict, Field

from common.data import DataEntity, DataLabel, DataSource


class XContent(BaseModel):
    """The content model for tweets.

    The model helps standardize the data format for tweets, even if they're scraped using different methods.
    """

    # Ignore extra fields since we only care about the fields defined in this model.
    model_config = ConfigDict(extra="ignore")

    username: str
    text: str
    url: str
    timestamp: dt.datetime
    tweet_hashtags: List[str] = Field(
        default_factory=list,
        description="A list of hashtags associated with the tweet, in order they appear in the tweet. Note: it's critical this ordering is respected as the first tag is used as the DataLabel for the index.",
    )

    @classmethod
    def to_data_entity(cls, content: "XContent") -> DataEntity:
        """Converts the XContent to a DataEntity."""

        content_bytes = content.json().encode("utf-8")
        return DataEntity(
            uri=content.url,
            datetime=content.timestamp,
            source=DataSource.X,
            label=(
                DataLabel(value=content.tweet_hashtags[0])
                if content.tweet_hashtags
                else None
            ),
            content=content_bytes,
            content_size_bytes=len(content_bytes),
        )

    @classmethod
    def from_data_entity(cls, data_entity: DataEntity) -> "XContent":
        """Converts a DataEntity to an XContent."""

        return XContent.parse_raw(data_entity.content.decode("utf-8"))
