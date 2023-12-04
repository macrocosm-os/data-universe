# The MIT License (MIT)
# Copyright © 2023 data-universe

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the “Software”), to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or substantial portions of
# the Software.

# THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
# THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

import typing
import bittensor as bt
from common.data import DataChunkSummary, DataEntity
from typing import List


# TODO: Delete the Dummy protocol
class Dummy(bt.Synapse):
    """
    A simple dummy protocol representation which uses bt.Synapse as its base.
    This protocol helps in handling dummy request and response communication between
    the miner and the validator.

    Attributes:
    - dummy_input: An integer value representing the input request sent by the validator.
    - dummy_output: An optional integer value which, when filled, represents the response from the miner.
    """

    # Required request input, filled by sending dendrite caller.
    dummy_input: int

    # Optional request output, filled by recieving axon.
    dummy_output: typing.Optional[int] = None

    def deserialize(self) -> int:
        """
        Deserialize the dummy output. This method retrieves the response from
        the miner in the form of dummy_output, deserializes it and returns it
        as the output of the dendrite.query() call.

        Returns:
        - int: The deserialized response, which in this case is the value of dummy_output.

        Example:
        Assuming a Dummy instance has a dummy_output value of 5:
        >>> dummy_instance = Dummy(dummy_input=4)
        >>> dummy_instance.dummy_output = 5
        >>> dummy_instance.deserialize()
        5
        """
        return self.dummy_output

class GetDataChunkIndexFromMiner(bt.Synapse):
    """
    Protocol by which Validators can retrieve the DataChunk Index from a Miner.

    Attributes:
    - data_chunk_summaries: A list of DataChunkSummary objects that the Miner can serve.
    """

    # Required request output, filled by recieving axon.
    data_chunk_summaries: List[DataChunkSummary]

    def deserialize(self) -> int:
        """
        Deserialize the data_chunk_summaries output. This method retrieves the response from
        the miner in the form of data_chunk_summaries, deserializes it and returns it
        as the output of the dendrite.query() call.

        Returns:
        - List[DataChunkSummary]: The deserialized response, which in this case is the value of data_chunk_summaries.
        """
        return self.data_chunk_summaries

class GetDataChunkFromMiner(bt.Synapse):
    """
    Protocol by which Validators can retrieve the DataEntities of a Chunk from a Miner.

    Attributes:
    - data_chunk: The DataChunk that the requester is asking for.
    - data_entities: A list of DataEntity objects that make up the requested DataChunk.
    """
    # Required request input, filled by sending dendrite caller.
    data_chunk: DataChunkSummary

    # Required request output, filled by recieving axon.
    data_entities: List[DataEntity]

    def deserialize(self) -> int:
        """
        Deserialize the data_entities output. This method retrieves the response from
        the miner in the form of data_entities, deserializes it and returns it
        as the output of the dendrite.query() call.

        Returns:
        - List[DataChunkSummary]: The deserialized response, which in this case is the value of data_entities.
        """
        return self.data_entities

# TODO Protocol for Users to Query Data which will accept query parameters such as a startDatetime, endDatetime.
