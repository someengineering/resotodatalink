from collections import defaultdict
from logging import getLogger
from typing import Dict, List, Tuple, Set

from resotoclient import Kind, Model
from resotolib.baseplugin import BaseCollectorPlugin
from resotolib.baseresources import BaseResource, EdgeType
from resotolib.core.actions import CoreFeedback
from resotolib.core.model_export import node_to_dict
from resotolib.json import from_json
from resotolib.types import Json
from sqlalchemy.engine import Engine

from resotodatalink.arrow.config import ArrowOutputConfig
from resotodatalink.sql import sql_updater

try:
    from resotodatalink.arrow.model import ArrowModel
    from resotodatalink.arrow.writer import ArrowWriter
except ImportError:
    pass


log = getLogger("resoto.datalink")


def prepare_node(node: BaseResource, collector: BaseCollectorPlugin) -> Json:
    node._graph = collector.graph
    exported = node_to_dict(node)
    exported["type"] = "node"
    exported["ancestors"] = {
        "cloud": {"reported": {"id": node.cloud().name}},
        "account": {"reported": {"id": node.account().name}},
        "region": {"reported": {"id": node.region().name}},
        "zone": {"reported": {"id": node.zone().name}},
    }
    return exported


def collect_to_file(
    collector: BaseCollectorPlugin, feedback: CoreFeedback, output_config: ArrowOutputConfig
) -> Tuple[str, int, int]:
    # collect cloud data
    feedback.progress_done(collector.cloud, 0, 1)
    collector.collect()
    # read the kinds created from this collector
    kinds = [from_json(m, Kind) for m in collector.graph.export_model(walk_subclasses=False)]
    model = ArrowModel(Model({k.fqn: k for k in kinds}), output_config.format)
    node_edge_count = len(collector.graph.nodes) + len(collector.graph.edges)
    ne_current = 0
    progress_update = max(node_edge_count // 100, 1)
    feedback.progress_done("sync_db", 0, node_edge_count, context=[collector.cloud])

    # group all edges by kind of from/to
    edges_by_kind: Set[Tuple[str, str]] = set()
    for from_node, to_node, key in collector.graph.edges:
        if key.edge_type == EdgeType.default:
            edges_by_kind.add((from_node.kind, to_node.kind))
    # create the ddl metadata from the kinds
    model.create_schema(list(edges_by_kind))
    # ingest the data
    writer = ArrowWriter(model, output_config)
    node: BaseResource
    for node in sorted(collector.graph.nodes, key=lambda n: n.kind):  # type: ignore
        exported = prepare_node(node, collector)
        writer.insert_node(exported)
        ne_current += 1
        if ne_current % progress_update == 0:
            feedback.progress_done("sync_db", ne_current, node_edge_count, context=[collector.cloud])
    for from_node, to_node, key in collector.graph.edges:
        if key.edge_type == EdgeType.default:
            writer.insert_node({"from": from_node.chksum, "to": to_node.chksum, "type": "edge"})
            ne_current += 1
            if ne_current % progress_update == 0:
                feedback.progress_done("sync_db", ne_current, node_edge_count, context=[collector.cloud])

    writer.close()

    feedback.progress_done(collector.cloud, 1, 1)
    return collector.cloud, len(collector.graph.nodes), len(collector.graph.edges)


def collect_sql(collector: BaseCollectorPlugin, engine: Engine, feedback: CoreFeedback) -> Tuple[str, int, int]:
    # collect cloud data
    feedback.progress_done(collector.cloud, 0, 1)
    collector.collect()

    # group all nodes by kind
    nodes_by_kind: Dict[str, List[Json]] = defaultdict(list)
    node: BaseResource
    for node in collector.graph.nodes:
        # create an exported node with the same scheme as resotocore
        exported = prepare_node(node, collector)
        nodes_by_kind[node.kind].append(exported)

    # group all edges by kind of from/to
    edges_by_kind: Dict[Tuple[str, str], List[Json]] = defaultdict(list)
    for from_node, to_node, key in collector.graph.edges:
        if key.edge_type == EdgeType.default:
            edge_node = {"from": from_node.chksum, "to": to_node.chksum, "type": "edge"}
            edges_by_kind[(from_node.kind, to_node.kind)].append(edge_node)

    # read the kinds created from this collector
    kinds = [from_json(m, Kind) for m in collector.graph.export_model(walk_subclasses=False)]
    updater = sql_updater(Model({k.fqn: k for k in kinds}), engine)
    node_edge_count = len(collector.graph.nodes) + len(collector.graph.edges)
    ne_count = 0
    schema = f"create temp tables {engine.dialect.name}"
    syncdb = f"synchronize {engine.dialect.name}"
    feedback.progress_done(schema, 0, 1, context=[collector.cloud])
    feedback.progress_done(syncdb, 0, node_edge_count, context=[collector.cloud])
    with engine.connect() as conn:
        with conn.begin():
            # create the ddl metadata from the kinds
            updater.create_schema(conn, list(edges_by_kind.keys()))
            feedback.progress_done(schema, 1, 1, context=[collector.cloud])

            # insert batches of nodes by kind
            for kind, nodes in nodes_by_kind.items():
                log.info(f"Inserting {len(nodes)} nodes of kind {kind}")
                for insert in updater.insert_nodes(kind, nodes):
                    conn.execute(insert)
                ne_count += len(nodes)
                feedback.progress_done(syncdb, ne_count, node_edge_count, context=[collector.cloud])

            # insert batches of edges by from/to kind
            for from_to, nodes in edges_by_kind.items():
                log.info(f"Inserting {len(nodes)} edges from {from_to[0]} to {from_to[1]}")
                for insert in updater.insert_edges(from_to, nodes):
                    conn.execute(insert)
                ne_count += len(nodes)
                feedback.progress_done(syncdb, ne_count, node_edge_count, context=[collector.cloud])
    feedback.progress_done(collector.cloud, 1, 1)
    return collector.cloud, len(collector.graph.nodes), len(collector.graph.edges)
