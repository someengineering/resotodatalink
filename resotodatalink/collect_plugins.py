import asyncio
from logging import getLogger
from typing import List, Tuple, Set, Optional, AsyncIterator, Union, cast, Dict, TypeVar

import jsons
from resotoclient import Kind, Model
from resotolib.baseplugin import BaseCollectorPlugin
from resotolib.baseresources import BaseResource, EdgeType
from resotolib.core.actions import CoreFeedback
from resotolib.types import Json
from sqlalchemy.engine import Engine

from resotodatalink.arrow.config import ArrowOutputConfig
from resotodatalink.batch_stream import BatchStream
from resotodatalink.sql import sql_updater, SqlUpdater

try:
    from resotodatalink.arrow.model import ArrowModel
    from resotodatalink.arrow.writer import ArrowWriter
except ImportError:
    pass


log = getLogger("resoto.datalink")


def collect_to_file(
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
    asyncio.run(write_file(output_config, model, stream, edges_by_kind, feedback, node_edge_count))

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


def collect_sql(
    collector: BaseCollectorPlugin, engine: Engine, feedback: CoreFeedback, swap_temp_tables: bool = False
) -> Tuple[str, int, int]:
    # collect cloud data
    feedback.progress_done(collector.cloud, 0, 1)
    collector.collect()

    # read the kinds created from this collector
    # Note: Kind is a dataclass - from_json can only handle attrs
    kinds = [jsons.load(m, Kind) for m in collector.graph.export_model(walk_subclasses=False)]
    updater = sql_updater(Model({k.fqn: k for k in kinds}), engine)

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

    stream = BatchStream.from_graph(collector, key_fn, updater.batch_size, len(collector.graph.nodes))
    asyncio.run(
        update_sql(
            engine,
            updater,
            stream,
            edges_by_kind,
            swap_temp_tables,
            feedback.with_context(collector.cloud),
            len(collector.graph.nodes) + len(collector.graph.edges),
        )
    )
    feedback.progress_done(collector.cloud, 1, 1)
    return collector.cloud, len(collector.graph.nodes), len(collector.graph.edges)


async def update_sql(
    engine: Engine,
    updater: SqlUpdater,
    elements: AsyncIterator[Tuple[Union[str, Tuple[str, str]], List[Json]]],
    all_edge_kinds: Set[Tuple[str, str]],
    swap_temp_tables: bool,
    feedback: Optional[CoreFeedback] = None,
    node_edge_count: Optional[int] = None,
) -> None:
    """
    Provide a stream of elements to update the sql database.
    The elements are
      - grouped by kind of from/to for edges: (instance, volume) -> { type=edge, from=i1, to=v1 ... }
      - grouped by kind for nodes: instance -> { type=node, reported= ... }
    :param engine: the sql engine to use.
    :param updater: the sql updater to use.
    :param feedback: the core feedback to use.
    :param elements: the elements to update (see above)
    :param all_edge_kinds: used to create the link table schema.
    :param swap_temp_tables: if True, swap the temp tables with the main tables.
    :param node_edge_count: only used for reporting progress. The total number of nodes and edges.
    """

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
            updater.swap_temp_tables(conn)


T = TypeVar("T")
