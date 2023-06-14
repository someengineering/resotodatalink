from collections import defaultdict
from itertools import chain
from typing import AsyncIterator, Any, Callable, Dict, List, Iterator, Union

from resotolib.baseplugin import BaseCollectorPlugin
from resotolib.baseresources import EdgeType

from resotodatalink.schema_utils import prepare_node, prepare_edge


async def sync_to_async_iterator(it: Iterator[Any]) -> AsyncIterator[Any]:
    for elem in it:
        yield elem


class BatchStream(AsyncIterator[Any]):
    """
    This class is used to batch elements from a stream based on a key function.
    It emits elements as batches with the same key.
    The batch size is configurable - when the batch size is reached, the batch is emitted.
    The watermark is used to define the maximum number of elements that can be stored in the stream.
    If the watermark hits, the stream will emit the largest batch even if the batch size is bigger.
    """

    def __init__(
        self,
        in_stream: Union[Iterator[Any], AsyncIterator[Any]],
        key_fn: Callable[[Any], Any],
        batch_size: int,
        watermark: int,
    ):
        self.in_stream = in_stream if isinstance(in_stream, AsyncIterator) else sync_to_async_iterator(in_stream)
        self.batch_size = batch_size
        self.key_fn = key_fn
        self.elements_by_key: Dict[Any, List[Any]] = defaultdict(list)
        self.watermark = watermark
        self.count = 0

    async def __anext__(self) -> Any:
        try:
            while True:
                elem = await self.in_stream.__anext__()
                self.count += 1
                key = self.key_fn(elem)
                self.elements_by_key[key].append(elem)
                if len(self.elements_by_key[key]) >= self.batch_size:
                    # batch size reached, return batch
                    batch = self.elements_by_key.pop(key)
                    self.count -= len(batch)
                    return key, batch
                elif self.count >= self.watermark:
                    # watermark reached, return the batch with the highest number of elements
                    key = max(self.elements_by_key, key=lambda k: len(self.elements_by_key[k]))
                    batch = self.elements_by_key.pop(key)
                    self.count -= len(batch)
                    return key, batch
        except StopAsyncIteration:
            # in stream exhausted, return remaining elements
            if self.elements_by_key:
                key, batch = self.elements_by_key.popitem()
                self.count -= len(batch)
                return key, batch
            else:
                raise

    @staticmethod
    def from_graph(
        collector: BaseCollectorPlugin, key_fn: Callable[[Any], Any], batch_size: int, watermark: int
    ) -> "BatchStream":
        return BatchStream(
            chain(
                (prepare_node(node, collector) for node in collector.graph.nodes),
                (
                    prepare_edge(from_node, to_node, key.edge_type)
                    for from_node, to_node, key in collector.graph.edges
                    if key.edge_type == EdgeType.default
                ),
            ),
            key_fn,
            batch_size,
            watermark,
        )
