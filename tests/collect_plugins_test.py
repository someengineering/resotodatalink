import csv
import pathlib
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, List

import pytest
from resoto_plugin_example_collector import ExampleCollectorPlugin
from resotolib.core.actions import CoreFeedback
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import Session

from resotodatalink import EngineConfig
from resotodatalink.arrow.config import ArrowOutputConfig, FileDestination
from resotodatalink.collect_plugins import collect_sql, collect_to_file, execute_sql


@pytest.mark.asyncio
async def test_collect_sql(core_feedback: CoreFeedback) -> None:
    with TemporaryDirectory() as tmp:
        engine_config = EngineConfig("sqlite:///" + tmp + "/test.db")
        await collect_sql(ExampleCollectorPlugin(), engine_config, core_feedback, True)
        engine = create_engine(engine_config.connection_string)
        # get all tables
        metadata = MetaData()
        metadata.reflect(bind=engine)
        expected_counts = {
            "example_account": 1,
            "example_custom_resource": 1,
            "example_instance": 2,
            "example_network": 2,
            "example_region": 2,
            "example_volume": 2,
            "link_example_account_example_region": 2,
            "link_example_instance_example_volume": 2,
            "link_example_network_example_instance": 2,
            "link_example_region_example_custom_resource": 1,
            "link_example_region_example_instance": 2,
            "link_example_region_example_network": 2,
            "link_example_region_example_volume": 2,
            "resource_short_property_access": 0,
        }
        assert set(metadata.tables.keys()) == set(expected_counts.keys())  # check that there are entries in the tables
        with Session(engine) as session:
            for table in metadata.tables.values():
                assert session.query(table).count() == expected_counts[table.name]


@pytest.mark.asyncio
async def test_collect_csv(core_feedback: CoreFeedback) -> None:
    def load_csv(path: Path) -> Dict[str, List[List[str]]]:
        """Load a folder with csv files into a dictionary"""
        result = {
            csv_path.name.strip(".csv"): list(csv.reader(open(csv_path)))[1:]
            for csv_path in pathlib.Path(path).glob("*.csv")
        }
        return result

    with TemporaryDirectory() as tmp:
        csv_output = Path(f"{tmp}/csv_output")
        cfg = ArrowOutputConfig(destination=FileDestination(csv_output), batch_size=1000, format="csv")
        await collect_to_file(ExampleCollectorPlugin(), core_feedback, cfg)
        counts = {name: len(lines) for name, lines in load_csv(csv_output).items()}
        expected_counts = {
            "example_account": 1,
            "example_custom_resource": 1,
            "example_instance": 2,
            "example_network": 2,
            "example_region": 2,
            "example_volume": 2,
            "link_example_account_example_region": 2,
            "link_example_instance_example_volume": 2,
            "link_example_network_example_instance": 2,
            "link_example_region_example_custom_resource": 1,
            "link_example_region_example_instance": 2,
            "link_example_region_example_network": 2,
            "link_example_region_example_volume": 2,
        }
        assert counts == expected_counts


@pytest.mark.asyncio
async def test_select(core_feedback: CoreFeedback) -> None:
    with TemporaryDirectory() as tmp:
        engine_config = EngineConfig("sqlite:///" + tmp + "/test.db")
        await collect_sql(ExampleCollectorPlugin(), engine_config, core_feedback, True)
        rows = [
            row
            async for row in execute_sql(
                engine_config, "select id, instance_cores, instance_memory from example_instance"
            )
        ]
        assert rows == [
            {"id": "someInstance1", "instance_cores": 4.0, "instance_memory": 32.0},
            {"id": "someInstance2", "instance_cores": 0.0, "instance_memory": 0.0},
        ]
