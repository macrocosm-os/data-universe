import json
from typing import Dict, Tuple
from dynamic_desirability.desirability_uploader import run_uploader_from_gravity
import bittensor as bt
import pydantic
from neurons.config import NeuronType, check_config, create_config


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

async def priority_organic_fn(synapse: OrganicProtocol) -> float:
    """Priority function for the axon."""
    return 10000000.0 # todo what does this number mean ?


async def blacklist_organic_fn(synapse: OrganicProtocol) -> Tuple[bool, str]:
    """Blacklist function for the axon."""
    # ! DO NOT CHANGE `Tuple` return type to `tuple`, it will break the code (bittensor internal signature checks).
    # We expect the API to be run with one specific hotkey (e.g. OTF).
    return False, 'no blacklist'  # TODO what key to add ?


async def on_organic_entry(synapse: OrganicProtocol):
    """Organic query handle."""
    # TODO desirabilty uploader
    config = create_config(NeuronType.VALIDATOR)

    await run_uploader_from_gravity(config, synapse.gravity)



def start_organic(axon: bt.axon):
    axon.attach(
        forward_fn=on_organic_entry,
        blacklist_fn=blacklist_organic_fn,
        priority_fn=priority_organic_fn,
    )