import csv
import pathlib
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, List

from resoto_plugin_example_collector import ExampleCollectorPlugin
from resotolib.core.actions import CoreFeedback
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import Session

from resotodatalink.arrow.config import ArrowOutputConfig, FileDestination
from resotodatalink.collect_plugins import collect_sql, collect_to_file


def test_collect_sql(core_feedback: CoreFeedback) -> None:
    with TemporaryDirectory() as tmp:
        engine = create_engine("sqlite:///" + tmp + "/test.db")
        collect_sql(ExampleCollectorPlugin(), engine, core_feedback, True)
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
        }
        assert set(metadata.tables.keys()) == expected_counts.keys()  # check that there are entries in the tables
        with Session(engine) as session:
            for table in metadata.tables.values():
                assert session.query(table).count() == expected_counts[table.name]


def test_collect_csv(core_feedback: CoreFeedback) -> None:
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
        collect_to_file(ExampleCollectorPlugin(), core_feedback, cfg)
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
