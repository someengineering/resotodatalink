from itertools import chain

from resotoclient.models import Model
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from sqlalchemy.types import VARCHAR, Text

from resotodatalink.sql import SqlDefaultUpdater


def test_create_schema(model: Model, engine: Engine) -> None:
    updater: SqlDefaultUpdater = SqlDefaultUpdater(model)
    with engine.connect() as connection:
        updater.create_schema(connection, [])

    info = MetaData()
    info.reflect(bind=engine)
    assert info.tables.keys() == {"tmp_some_instance", "tmp_some_volume", "tmp_link_some_instance_some_volume"}
    si = info.tables["tmp_some_instance"].columns
    assert set(si.keys()) == {
        "_id",
        "id",
        "cores",
        "memory",
        "name",
        "alias",
        "description",
        "cloud",
        "account",
        "region",
        "zone",
    }
    assert isinstance(si["name"].type, VARCHAR)
    assert si["name"].type.length == 64  # len is 34
    assert isinstance(si["alias"].type, VARCHAR)
    assert si["alias"].type.length == 255  # len is not defined
    assert isinstance(si["description"].type, Text)  # len is 1500
    assert set(info.tables["tmp_some_volume"].columns.keys()) == {
        "_id",
        "id",
        "capacity",
        "name",
        "alias",
        "description",
        "cloud",
        "account",
        "region",
        "zone",
    }
    assert set(info.tables["tmp_link_some_instance_some_volume"].columns.keys()) == {"to_id", "from_id"}


def test_swap_temp_tables(model: Model, engine: Engine) -> None:
    updater: SqlDefaultUpdater = SqlDefaultUpdater(model)
    with engine.connect() as connection:
        updater.create_schema(connection, [])
        # no temp table - swap should not do anything
        updater.swap_temp_tables(connection, drop_existing_tables=False)
        metadata = MetaData()
        metadata.reflect(connection, resolve_fks=False)
        assert len(metadata.tables.values()) == 3
        # no temp table - swap should drop all existing tables
        updater.swap_temp_tables(connection, drop_existing_tables=True)
        metadata = MetaData()
        metadata.reflect(connection, resolve_fks=False)
        assert len(metadata.tables.values()) == 0


def test_update(engine_with_schema: Engine, updater: SqlDefaultUpdater) -> None:
    instance = {
        "type": "node",
        "id": "i-123",
        "reported": {
            "kind": "some_instance",
            "id": "i-123",
            "name": "in1",
            "alias": "t1",
            "description": "h1",
            "cores": 4,
            "memory": 8,
        },
        "ancestors": {
            "cloud": {"reported": {"id": "some_cloud"}},
            "account": {"reported": {"id": "some_account"}},
            "region": {"reported": {"id": "some_region"}},
            "zone": {"reported": {"id": "some_zone"}},
        },
    }
    volume = {
        "type": "node",
        "id": "v-123",
        "reported": {
            "kind": "some_volume",
            "id": "v-123",
            "name": "vol1",
            "alias": "t1",
            "description": "h1",
            "capacity": 12,
        },
        "ancestors": {
            "cloud": {"reported": {"id": "some_cloud"}},
            "account": {"reported": {"id": "some_account"}},
            "region": {"reported": {"id": "some_region"}},
            "zone": {"reported": {"id": "some_zone"}},
        },
    }

    with Session(engine_with_schema) as session:
        for stmt in chain(
            updater.insert_nodes("some_instance", [instance]),
            updater.insert_nodes("some_volume", [volume]),
            updater.insert_edges(("some_instance", "some_volume"), [{"type": "edge", "from": "i-123", "to": "v-123"}]),
        ):
            session.execute(stmt)

        # one instance is persisted
        row1 = ("i-123", 4, 8, "i-123", "in1", "t1", "h1", "some_cloud", "some_account", "some_region", "some_zone")
        assert session.query(updater.metadata.tables["tmp_some_instance"]).all() == [row1]

        # one volume is persisted
        row2 = ("v-123", 12, "v-123", "vol1", "t1", "h1", "some_cloud", "some_account", "some_region", "some_zone")
        assert session.query(updater.metadata.tables["tmp_some_volume"]).all() == [row2]

        # link from instance to volume is persisted
        assert session.query(updater.metadata.tables["tmp_link_some_instance_some_volume"]).all() == [
            ("i-123", "v-123")
        ]
