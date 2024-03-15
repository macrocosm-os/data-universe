import datetime as dt
import json
from re import X
from typing import List
from pydantic import BaseModel, ConfigDict, Field

from common import constants
from common.data import DataEntity, DataLabel, DataSource
from scraping import utils


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
    def to_data_entity(
        cls, content: "XContent", obfuscate_content_date: bool
    ) -> DataEntity:
        """Converts the XContent to a DataEntity."""
        entity_timestamp = content.timestamp
        if obfuscate_content_date:
            content.timestamp = utils.obfuscate_datetime_to_minute(entity_timestamp)

        content_bytes = content.json().encode("utf-8")
        return DataEntity(
            uri=content.url,
            datetime=entity_timestamp,
            source=DataSource.X,
            label=(
                DataLabel(
                    value=content.tweet_hashtags[0].lower()[
                        : constants.MAX_LABEL_LENGTH
                    ]
                )
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
