import datetime as dt
import json
from re import X
from typing import Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from common import constants
from common.data import DataEntity, DataLabel, DataSource
from scraping import utils


class XContent(BaseModel):
    """The content model for tweets.

    The model helps standardize the data format for tweets, even if they're scraped using different methods.
    """

    class Config:
        extra = "forbid"

    # model_config should NOT be set by Miners.
    model_config: Dict[str, str] = Field(default=None)

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
        entity_timestamp = content.timestamp
        content.timestamp = utils.obfuscate_datetime_to_minute(entity_timestamp)
        content_bytes = content.json(exclude_none=True).encode("utf-8")

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


        content_str = data_entity.content.decode("utf-8")
        content_dict = json.loads(content_str)

        if 'is_retweet' in content_dict:
            if not isinstance(content_dict['is_retweet'], bool):
                # Remove 'is_retweet' if it's not a boolean and raise a more appropriate exception
                content_dict.pop('is_retweet', None)
                raise ValueError("The 'is_retweet' field must be a boolean.")  # Using ValueError for clearer intent
            else:
                # remove 'is_retweet'
                content_dict.pop('is_retweet', None)

        clean_content_str = json.dumps(content_dict)
        return XContent.parse_raw(clean_content_str)
