import logging
from abc import ABC, abstractmethod
from datetime import datetime, date
from typing import List, Any, Type, Tuple, Dict, Iterator, Optional, Union

from resotoclient.models import Kind, Model, Property
from resotolib.types import Json
from resotolib.utils import UTC_Date_Format
from sqlalchemy import (
    Boolean,
    Column,
    Float,
    Integer,
    JSON,
    MetaData,
    String,
    Table,
    Text,
    DDL,
    DateTime,
    Date,
    TypeDecorator,
)
from sqlalchemy.engine import Engine, Connection, Dialect
from sqlalchemy.sql.ddl import DropTable, DropConstraint
from sqlalchemy.sql.dml import ValuesBase

from resotodatalink import EngineConfig
from resotodatalink.schema_utils import (
    base_kinds,
    temp_prefix,
    carz_access,
    get_table_name,
    get_link_table_name,
    kind_properties,
)
from resotolib.json import value_in_path
from sqlalchemy import create_engine

log = logging.getLogger("resoto.datalink")


class DateTimeString(TypeDecorator):  # type: ignore
    """
    This type decorator translates between string (python) and datetime (sqlalchemy) types.
    """

    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value: Optional[str], dialect: Dialect) -> Optional[datetime]:
        return datetime.strptime(value, UTC_Date_Format) if value else None

    def process_result_value(self, value: Optional[datetime], dialect: Dialect) -> Optional[str]:
        return value.strftime(UTC_Date_Format) if value else None


class DateString(TypeDecorator):  # type: ignore
    """
    This type decorator translates between string (python) and date (sqlalchemy) types.
    """

    impl = Date
    cache_ok = True

    def process_bind_param(self, value: Optional[str], dialect: Dialect) -> Optional[date]:
        return date.fromisoformat(value) if value else None

    def process_result_value(self, value: Optional[datetime], dialect: Dialect) -> Optional[str]:
        return value.strftime("%Y-%m-%d") if value else None


def sql_kind_to_column_type(kind_name: str, model: Model, **kwargs: Any) -> Any:  # Type[TypeEngine[Any]]
    kind = model.kinds.get(kind_name)
    if "[]" in kind_name:
        return JSON
    elif kind_name.startswith("dict"):
        return JSON
    elif kind_name == "any":
        return JSON
    elif kind_name in ("int32", "int64"):
        return Integer
    elif kind_name == "float":
        return Float
    elif kind_name == "double":
        return Float  # use Double with sqlalchemy 2
    elif kind_name == "datetime":
        return DateTimeString
    elif kind_name == "date":
        return DateString
    elif kind_name in ("string", "duration"):
        if isinstance(str_len := kwargs.get("len"), int):
            if str_len > 1024:
                return Text
            elif str_len < 32:
                return String(32)
            elif str_len >= 1:
                return String(2 ** (str_len - 1).bit_length())
        # if we come here no or invalid len was provided, default to 255 characters
        return String(255)
    elif kind_name == "boolean":
        return Boolean
    elif kind and kind.properties is not None:  # complex kind
        return JSON
    elif kind and kind.runtime_kind is not None:  # refined simple type like enum
        return sql_kind_to_column_type(kind.runtime_kind, model)
    else:
        raise ValueError(f"Not able to handle kind {kind_name}")


class SqlUpdater(ABC):
    @property
    @abstractmethod
    def batch_size(self) -> int:
        pass

    @abstractmethod
    def insert_nodes(self, kind: str, nodes: List[Json]) -> Iterator[ValuesBase]:
        pass

    @abstractmethod
    def insert_edges(self, from_to: Tuple[str, str], nodes: List[Json]) -> Iterator[ValuesBase]:
        pass

    @abstractmethod
    def create_schema(self, connection: Connection, edges: List[Tuple[str, str]]) -> MetaData:
        pass

    @staticmethod
    def swap_temp_tables(db_access: Union[EngineConfig, Connection], drop_existing_tables: bool = False) -> None:
        def swap(connection: Connection) -> None:
            metadata = MetaData()
            metadata.reflect(connection, resolve_fks=False)
            dropped_tables = set()

            def drop_table(tl: Table) -> None:
                if tl.name in dropped_tables:
                    return
                dropped_tables.add(tl.name)
                for cs in tl.foreign_key_constraints:
                    connection.execute(DropConstraint(cs))
                connection.execute(DropTable(tl))

            for table in metadata.tables.values():
                if table.name.startswith(temp_prefix):
                    prod_table = table.name[len(temp_prefix) :]  # noqa: E203
                    if prod_table in metadata.tables:
                        drop_table(metadata.tables[prod_table])
                    connection.execute(DDL(f"ALTER TABLE {table.name} RENAME TO {prod_table}"))
                elif drop_existing_tables:
                    drop_table(table)
            # todo: create foreign key constraints on the final tables

        if isinstance(db_access, EngineConfig):
            engine = create_engine(db_access.connection_string)
            with engine.begin() as connection:
                swap(connection)
        else:
            swap(db_access)


class SqlDefaultUpdater(SqlUpdater):
    def __init__(self, model: Model, **kwargs: Any) -> None:
        self.model = model
        self.metadata = MetaData()
        self.table_kinds = [
            kind
            for kind in model.kinds.values()
            if kind.aggregate_root and kind.runtime_kind is None and kind.fqn not in base_kinds
        ]
        self.kind_by_id: Dict[str, str] = {}
        self.column_types_fn = kwargs.get("kind_to_column_type", sql_kind_to_column_type)
        self.insert_batch_size: int = int(kwargs.get("insert_batch_size", 5000))

    @property
    def batch_size(self) -> int:
        return self.insert_batch_size

    def create_schema(self, connection: Connection, edges: List[Tuple[str, str]]) -> MetaData:
        log.info(f"Create schema for {len(self.table_kinds)} kinds and their relationships")

        def table_schema(kind: Kind) -> None:
            table_name = get_table_name(kind.fqn)
            if table_name not in self.metadata.tables:
                pr, _ = kind_properties(kind, self.model, with_id=False)
                columns = [Column(p.name, self.column_types_fn(p.kind, self.model, **(p.metadata or {}))) for p in pr]
                idc = Column("_id", String(255), primary_key=True)
                Table(get_table_name(kind.fqn), self.metadata, *[idc, *columns])

        def link_table_schema(from_kind: str, to_kind: str) -> None:
            from_table = get_table_name(from_kind)
            to_table = get_table_name(to_kind)
            link_table = get_link_table_name(from_kind, to_kind)
            if (
                link_table not in self.metadata.tables
                and from_table in self.metadata.tables
                and to_table in self.metadata.tables
            ):
                # defining a foreign key constraint on a table that does not exist yet, will fail
                # defining it on the current tmp table setup is possible, but this will not reset during table rename
                Table(link_table, self.metadata, Column("from_id", String(255)), Column("to_id", String(255)))

        def link_table_schema_from_successors(kind: Kind) -> None:
            _, successors = kind_properties(kind, self.model)
            # create link table for all linked entities
            for successor in successors:
                link_table_schema(kind.fqn, successor)

        # step 1: create tables for all kinds
        for kind in self.table_kinds:
            table_schema(kind)
        # step 2: create link tables for all kinds
        for kind in self.table_kinds:
            link_table_schema_from_successors(kind)
        # step 3: create link tables for all seen edges
        for from_kind, to_kind in edges:
            link_table_schema(from_kind, to_kind)

        # drop tables if requested
        self.metadata.drop_all(connection)

        # create the tables
        self.metadata.create_all(connection)

        return self.metadata

    def node_to_json(self, node: Json, known_props: Optional[List[Property]] = None) -> Json:
        if node.get("type") == "node" and "id" in node and "reported" in node:
            reported: Json = node.get("reported", {})
            reported["_id"] = node["id"]
            reported["cloud"] = value_in_path(node, carz_access["cloud"])
            reported["account"] = value_in_path(node, carz_access["account"])
            reported["region"] = value_in_path(node, carz_access["region"])
            reported["zone"] = value_in_path(node, carz_access["zone"])
            # In case of known props, make sure to always return a dict with all known props
            if known_props is not None:
                return {p.name: reported.get(p.name) for p in known_props}
            else:
                reported.pop("kind", None)
                return reported
        elif node.get("type") == "edge" and "from" in node and "to" in node:
            return {"from_id": node["from"], "to_id": node["to"]}
        raise ValueError(f"Unknown node: {node}")

    def insert_nodes(self, kind: str, nodes: List[Json]) -> Iterator[ValuesBase]:
        # create a defaults dict with all properties set to None
        kp, _ = kind_properties(self.model.kinds[kind], self.model, with_id=True)

        if (table := self.metadata.tables.get(get_table_name(kind))) is not None:
            for batch in (nodes[i : i + self.insert_batch_size] for i in range(0, len(nodes), self.insert_batch_size)):
                converted = [self.node_to_json(node, kp) for node in batch]
                yield table.insert().values(converted)

    def insert_edges(self, from_to: Tuple[str, str], nodes: List[Json]) -> Iterator[ValuesBase]:
        from_kind, to_kind = from_to
        table = self.metadata.tables.get(get_link_table_name(from_kind, to_kind))
        maybe_insert = table.insert() if table is not None else None
        if maybe_insert is not None:
            for batch in (nodes[i : i + self.insert_batch_size] for i in range(0, len(nodes), self.insert_batch_size)):
                converted = [self.node_to_json(node) for node in batch]
                yield maybe_insert.values(converted)


# register your updater by dialect name here
DialectUpdater: Dict[str, Type[SqlUpdater]] = {}


def sql_updater(model: Model, engine: Engine) -> SqlUpdater:
    updater_class: Type[SqlUpdater] = DialectUpdater.get(engine.dialect.name, SqlDefaultUpdater)
    updater: SqlUpdater = updater_class(model)  # type: ignore
    log.info(f"Dialect {engine.dialect.name}: Use updater class {updater_class.__name__}")
    return updater
