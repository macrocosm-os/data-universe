import datetime as dt
import json
from re import X
from typing import Dict, List
from pydantic import BaseModel, ConfigDict, Field

from common import constants
from common.data import DataEntity, DataLabel, DataSource
from scraping.x.classifiers import TweetLabeler
from scraping import utils


class XContent(BaseModel):
    """The content model for tweets.

    The model helps standardize the data format for tweets, even if they're scraped using different methods.
    """

    class Config:
        extra = "forbid"

    # model_config should NOT be set by Miners.
    # In the near future, Validators will penalized Miners who set this field.
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
    def to_data_entity(cls, content: "XContent", labeler: "TweetLabeler") -> DataEntity:
        """Converts the XContent to a DataEntity."""
        entity_timestamp = content.timestamp
        content.timestamp = utils.obfuscate_datetime_to_minute(entity_timestamp)
        content_bytes = content.json(exclude_none=True).encode("utf-8")
        entity_generated_label = False
        entity_label = None

        if not content.tweet_hashtags and labeler.check_english(content.text):
            # Generate a label from the tweet content
            entity_label = DataLabel(value = labeler.label_tweet_singular(text = content.text).lower()[:constants.MAX_LABEL_LENGTH])
            entity_generated_label = True
        elif content.tweet_hashtags:
            entity_label = DataLabel(value = content.tweet_hashtags[0].lower()[:constants.MAX_LABEL_LENGTH])

        return DataEntity(
            uri=content.url,
            datetime=entity_timestamp,
            source=DataSource.X,
            label=entity_label,
            content=content_bytes,
            content_size_bytes=len(content_bytes),
            is_generated_label = entity_generated_label
        )

    @classmethod
    def from_data_entity(cls, data_entity: DataEntity) -> "XContent":
        """Converts a DataEntity to an XContent."""

        return XContent.parse_raw(data_entity.content.decode("utf-8"))
    
