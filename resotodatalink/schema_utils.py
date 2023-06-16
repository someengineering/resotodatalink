import inspect
from typing import List, Dict, Tuple

from resotoclient.models import Property, Kind, Model
from resotolib import baseresources
from resotolib.baseplugin import BaseCollectorPlugin
from resotolib.baseresources import BaseResource, EdgeType
from resotolib.core.model_export import node_to_dict
from resotolib.types import Json

# This set will hold the names of all "base" resources
# Since that are abstract classes, there will be no instances of them - hence we do not need a table for them.
base_kinds = {
    clazz.kind for _, clazz in inspect.getmembers(baseresources, inspect.isclass) if issubclass(clazz, BaseResource)
}

temp_prefix = "tmp_"

carz = [
    Property("cloud", "string"),
    Property("account", "string"),
    Property("region", "string"),
    Property("zone", "string"),
]
carz_access = {name: ["ancestors", name, "reported", "id"] for name in ["cloud", "account", "region", "zone"]}


def get_table_name(kind: str, with_tmp_prefix: bool = True) -> str:
    replaced = kind.replace(".", "_")
    return temp_prefix + replaced if with_tmp_prefix else replaced


def get_link_table_name(from_kind: str, to_kind: str, with_tmp_prefix: bool = True) -> str:
    # postgres table names are not allowed to be longer than 63 characters
    replaced = f"link_{get_table_name(from_kind, False)[0:25]}_{get_table_name(to_kind, False)[0:25]}"
    return temp_prefix + replaced if with_tmp_prefix else replaced


def kind_properties(kind: Kind, model: Model, with_id: bool = False) -> Tuple[List[Property], List[str]]:
    visited = set()

    def base_props_not_visited(kd: Kind) -> Tuple[List[Property], List[str]]:
        if kd.fqn in visited:
            return [], []
        visited.add(kd.fqn)
        # take all properties that are not synthetic
        # also ignore the kind property, since it is available in the table name
        properties: Dict[str, Property] = {
            prop.name: prop for prop in (kd.properties or []) if prop.synthetic is None and prop.name != "kind"
        }
        defaults = kd.successor_kinds.get("default") if kd.successor_kinds else None
        successors: List[str] = defaults.copy() if defaults else []
        for kind_name in kd.bases or []:
            if ck := model.kinds.get(kind_name):  # and kind_name != "resource":
                props, succs = base_props_not_visited(ck)
                for prop in props:
                    properties[prop.name] = prop
                successors.extend(succs)
        return list(properties.values()), successors

    prs, scs = base_props_not_visited(kind)
    id_prop = [Property("_id", "string", True)] if with_id else []
    return id_prop + prs + carz, scs


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


def prepare_edge(from_resource: BaseResource, to_resource: BaseResource, edge_type: EdgeType) -> Json:
    return {
        "type": "edge",
        "from": from_resource.chksum,
        "to": to_resource.chksum,
        "edge_type": edge_type.name,
    }
