import asyncio
import json
import time
from dataclasses import dataclass, field
from functools import partial
from typing import Dict, Tuple

import bittensor as bt
from bittensor.dendrite import dendrite
from loguru import logger
from starlette.types import Send
import pydantic


class OrganicProtocol(bt.Synapse):
    gravity: Dict[str, str] = pydantic.Field(
        title="gravity",
        description="accepts dict",
        frozen=True,
        repr=False,
        default_factory=dict,
    )

    @classmethod
    def from_json(cls, json_file: str):
        with open(json_file, 'r') as f:
            data = json.load(f)
        return cls(**data)

    def to_json(self, json_file: str):
        with open(json_file, 'w') as f:
            json.dump(self.dict(), f, indent=4)

async def priority_organic_fn(synapse: bt.Synapse) -> float:
    """Priority function for the axon."""
    return 10000000.0 # todo what does this number mean ?


async def blacklist_organic_fn(synapse: bt.Synapse) -> Tuple[bool, str]:
    """Blacklist function for the axon."""
    # ! DO NOT CHANGE `Tuple` return type to `tuple`, it will break the code (bittensor internal signature checks).
    # We expect the API to be run with one specific hotkey (e.g. OTF).
    return False, 'no blacklist'  # TODO what key to add ?


async def on_organic_entry(synapse: bt.Synapse) -> bt.Synapse:
    """Organic query handle."""
    if not isinstance(synapse, bt.Synapse):
        logger.error(f"[Organic] Received non-synapse task: {synapse.task_name}")
        return

    bt.logging.debug('Hello from organic')


def start_organic(axon: bt.axon):
    axon.attach(
        forward_fn=on_organic_entry,
        blacklist_fn=blacklist_organic_fn,
        priority_fn=priority_organic_fn,
    )