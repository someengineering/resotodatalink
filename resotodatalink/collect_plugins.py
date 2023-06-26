import asyncio
from concurrent.futures import ThreadPoolExecutor
from logging import getLogger
from typing import List, Tuple, Set, Optional, AsyncIterator, Union, cast, Dict, TypeVar, Awaitable, Any

import jsons
from resotoclient import Kind, Model
from resotolib.baseplugin import BaseCollectorPlugin
from resotolib.baseresources import BaseResource, EdgeType
from resotolib.core.actions import CoreFeedback
from resotolib.types import Json
from sqlalchemy import create_engine, text

from resotodatalink import EngineConfig
from resotodatalink.arrow.config import ArrowOutputConfig
from resotodatalink.batch_stream import BatchStream
from resotodatalink.sql import sql_updater

try:
    from resotodatalink.arrow.model import ArrowModel
    from resotodatalink.arrow.writer import ArrowWriter
except ImportError:
    ArrowModel = type(None)  # type: ignore
    ArrowWriter = type(None)  # type: ignore

log = getLogger("resoto.datalink")
T = TypeVar("T")


async def collect_to_file(
    collector: BaseCollectorPlugin, feedback: CoreFeedback, output_config: ArrowOutputConfig
) -> Tuple[str, int, int]:
    # collect cloud data
    feedback.progress_done(collector.cloud, 0, 1)
    collector.collect()
    # read the kinds created from this collector
    # Note: Kind is a dataclass - from_json can only handle attrs
    kinds = [jsons.load(m, Kind) for m in collector.graph.export_model(walk_subclasses=False)]
    model = ArrowModel(Model({k.fqn: k for k in kinds}), output_config.format)
    node_edge_count = len(collector.graph.nodes) + len(collector.graph.edges)
    feedback.progress_done("sync_db", 0, node_edge_count, context=[collector.cloud])

    # group all edges by kind of from/to
    edges_by_kind: Set[Tuple[str, str]] = set()
    for from_node, to_node, key in collector.graph.edges:
        if key.edge_type == EdgeType.default:
            edges_by_kind.add((from_node.kind, to_node.kind))

    # node lookup
    node_by_chksum: Dict[str, BaseResource] = {}
    for node in collector.graph.nodes:
        node_by_chksum[node.chksum] = node

    def key_fn(node: Json) -> Union[str, Tuple[str, str]]:
        if node["type"] == "edge":
            fr = node["from"]
            to = node["to"]
            return node_by_chksum[fr].kind, node_by_chksum[to].kind
        elif node["type"] == "node":
            return cast(str, node["reported"]["kind"])
        else:
            raise ValueError(f"Unknown node type {node['type']}")

    stream = BatchStream.from_graph(collector, key_fn, output_config.batch_size, len(collector.graph.nodes))
    await write_file(output_config, model, stream, edges_by_kind, feedback, node_edge_count)

    feedback.progress_done(collector.cloud, 1, 1)
    return collector.cloud, len(collector.graph.nodes), len(collector.graph.edges)


async def write_file(
    output_config: ArrowOutputConfig,
    model: ArrowModel,
    elements: AsyncIterator[Tuple[Union[str, Tuple[str, str]], List[Json]]],
    all_edge_kinds: Set[Tuple[str, str]],
    feedback: Optional[CoreFeedback] = None,
    node_edge_count: Optional[int] = None,
) -> None:
    async def run_on_thread() -> None:
        """
        PyArrow is not async, so we need to run it on a thread, to avoid blocking the event loop.
        """
        # create the ddl metadata from the kinds
        model.create_schema(list(all_edge_kinds))
        # ingest the data
        writer = ArrowWriter(model, output_config)
        try:
            ne_current = 0
            async for key, nodes in elements:
                if isinstance(key, str):  # a list of nodes
                    writer.insert_nodes(key, nodes)
                else:  # a list of edges
                    writer.insert_edges(key, nodes)
                ne_current += len(nodes)
                if feedback and node_edge_count:
                    feedback.progress_done("sync_db", ne_current, node_edge_count)
        finally:
            writer.close()

    await __run_on_thread(run_on_thread())


async def collect_sql(
    collector: BaseCollectorPlugin,
    engine_config: EngineConfig,
    feedback: CoreFeedback,
    swap_temp_tables: bool = False,
    drop_existing_tables: bool = False,
) -> Tuple[str, int, int]:
    # collect cloud data
    feedback.progress_done(collector.cloud, 0, 1)
    collector.collect()

    # read the kinds created from this collector
    # Note: Kind is a dataclass - from_json can only handle attrs
    kinds = [jsons.load(m, Kind) for m in collector.graph.export_model(walk_subclasses=False)]
    model = Model({k.fqn: k for k in kinds})

    # compute the available edge kinds
    edges_by_kind: Set[Tuple[str, str]] = set()
    for from_node, to_node, key in collector.graph.edges:
        if key.edge_type == EdgeType.default:
            edges_by_kind.add((from_node.kind, to_node.kind))

    # node lookup
    node_by_chksum: Dict[str, BaseResource] = {}
    for node in collector.graph.nodes:
        node_by_chksum[node.chksum] = node

    def key_fn(node: Json) -> Union[str, Tuple[str, str]]:
        if node["type"] == "edge":
            fr = node["from"]
            to = node["to"]
            return node_by_chksum[fr].kind, node_by_chksum[to].kind
        elif node["type"] == "node":
            return cast(str, node["reported"]["kind"])
        else:
            raise ValueError(f"Unknown node type {node['type']}")

    stream = BatchStream.from_graph(collector, key_fn, engine_config.batch_size, len(collector.graph.nodes))
    await update_sql(
        engine_config,
        model,
        stream,
        edges_by_kind,
        swap_temp_tables,
        drop_existing_tables,
        feedback.with_context(collector.cloud),
        len(collector.graph.nodes) + len(collector.graph.edges),
    )
    feedback.progress_done(collector.cloud, 1, 1)
    return collector.cloud, len(collector.graph.nodes), len(collector.graph.edges)


async def update_sql(
    engine_config: EngineConfig,
    model: Model,
    elements: AsyncIterator[Tuple[Union[str, Tuple[str, str]], List[Json]]],
    all_edge_kinds: Set[Tuple[str, str]],
    swap_temp_tables: bool,
    drop_existing_tables: bool,
    feedback: Optional[CoreFeedback] = None,
    node_edge_count: Optional[int] = None,
) -> None:
    """
    Provide a stream of elements to update the sql database.
    The elements are
      - grouped by kind of from/to for edges: (instance, volume) -> { type=edge, from=i1, to=v1 ... }
      - grouped by kind for nodes: instance -> { type=node, reported= ... }
    :param engine_config: the configuration for the engine to use
    :param model: the description of the data model
    :param feedback: the core feedback to use.
    :param elements: the elements to update (see above)
    :param all_edge_kinds: used to create the link table schema.
    :param swap_temp_tables: if True, swap the temp tables with the main tables.
    :param drop_existing_tables: if True, delete all existing tables that are not updated.
    :param node_edge_count: only used for reporting progress. The total number of nodes and edges.
    """

    async def run_on_thread() -> None:
        """
        IO Operations should not block the main thread.
        There is an async engine, but it has to be supported by the dialect (not all do).
        Using a thread pool is also not supported, since a lot of dialects require a single thread.
        Conclusion: we run the whole update on a single thread.
        """
        engine = create_engine(engine_config.connection_string)
        updater = sql_updater(model, engine)

        ne_count = 0
        schema = f"create temp tables {engine.dialect.name}"
        syncdb = f"synchronize {engine.dialect.name}"
        with engine.connect() as conn:
            with conn.begin():
                # create the ddl metadata from the kinds
                if feedback:
                    feedback.progress_done(schema, 0, 1)
                updater.create_schema(conn, list(all_edge_kinds))
                if feedback:
                    feedback.progress_done(schema, 1, 1)
                    if node_edge_count is not None:
                        feedback.progress_done(syncdb, 0, node_edge_count)
                async for key, nodes in elements:
                    if isinstance(key, str):  # a list of nodes
                        for insert in updater.insert_nodes(key, nodes):
                            conn.execute(insert)
                    else:  # a list of edges
                        for insert in updater.insert_edges(key, nodes):
                            conn.execute(insert)
                    ne_count += len(nodes)
                    if feedback and node_edge_count is not None:
                        feedback.progress_done(syncdb, ne_count, node_edge_count)

            if swap_temp_tables:
                updater.swap_temp_tables(conn, drop_existing_tables)

    await __run_on_thread(run_on_thread())


async def execute_sql(
    engine_config: EngineConfig, sql: str, bind_vars: Optional[Dict[str, Any]] = None
) -> AsyncIterator[Json]:
    engine = create_engine(engine_config.connection_string)
    with engine.connect() as conn:
        with conn.begin():
            result_set = conn.execute(text(sql), bind_vars)
            for row in result_set:
                yield dict(row)


async def __run_on_thread(awaitable: Awaitable[T]) -> T:
    """
    Run the async awaitable on a new thread.
    """

    def run_in_new_loop(lp: asyncio.AbstractEventLoop) -> T:
        asyncio.set_event_loop(lp)
        return lp.run_until_complete(awaitable)

    with ThreadPoolExecutor() as executor:
        loop = asyncio.new_event_loop()
        try:
            future = executor.submit(run_in_new_loop, loop)
            return await asyncio.wrap_future(future)
        finally:
            loop.stop()
            loop.close()
