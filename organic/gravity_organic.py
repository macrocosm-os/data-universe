import json
from typing import Dict, List
import pydantic
import bittensor as bt
from typing import Dict, Tuple
from dynamic_desirability.desirability_uploader import run_uploader_from_gravity
from neurons.config import NeuronType, check_config, create_config


class GravityItem(pydantic.BaseModel):
    source_name: str
    label_weights: Dict[str, float]

    class Config:
        frozen = True


class OrganicProtocol(bt.Synapse):
    gravity: str = pydantic.Field(
        title="gravity",
        description="String representation of list of source items with label weights",
        frozen=True,
        repr=False,
        default=""
    )

    @classmethod
    def from_json(cls, json_file: str):
        with open(json_file, 'r') as f:
            data = f.read()
            return cls(gravity=data)

    def to_json(self, json_file: str):
        with open(json_file, 'w') as f:
            f.write(self.gravity)

    def parse_gravity(self) -> List[GravityItem]:
        """Parse the gravity string into a list of GravityItems"""
        if not self.gravity:
            return []
        data = json.loads(self.gravity)
        return [GravityItem(**item) for item in data]

async def priority_organic_fn(synapse: OrganicProtocol) -> float:
    """Priority function for the axon."""
    return 10000000.0 # todo what does this number mean ?


async def blacklist_organic_fn(synapse: OrganicProtocol) -> Tuple[bool, str]:
    """Blacklist function for the axon."""
    # ! DO NOT CHANGE `Tuple` return type to `tuple`, it will break the code (bittensor internal signature checks).
    # We expect the API to be run with one specific hotkey (e.g. OTF).
    if synapse.dendrite.hotkey == '5Cg5QgjMfRqBC6bh8X4PDbQi7UzVRn9eyWXsB8gkyfppFPPy':
        return True, 'Non Macrocosmos request'
    return False, ''


async def on_organic_entry(synapse: OrganicProtocol):
    """Organic query handle."""
    config = create_config(NeuronType.VALIDATOR)

    await run_uploader_from_gravity(config, synapse.parse_gravity())


def start_organic(axon: bt.axon):
    axon.attach(
        forward_fn=on_organic_entry,
        blacklist_fn=blacklist_organic_fn,
        priority_fn=priority_organic_fn,
    )