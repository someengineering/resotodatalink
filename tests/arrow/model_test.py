from resotoclient.models import Model

from resotodatalink.arrow.model import ArrowModel


def test_create_schema(model: Model) -> None:
    parquet_model = ArrowModel(model, "parquet")
    parquet_model.create_schema([])

    assert parquet_model.schemas.keys() == {"some_instance", "some_volume", "link_some_instance_some_volume"}
    assert set(parquet_model.schemas["some_instance"].names) == {
        "_id",
        "id",
        "cores",
        "memory",
        "name",
        "cloud",
        "account",
        "region",
        "zone",
    }
    assert set(parquet_model.schemas["some_volume"].names) == {
        "_id",
        "id",
        "capacity",
        "name",
        "cloud",
        "account",
        "region",
        "zone",
    }
    assert set(parquet_model.schemas["link_some_instance_some_volume"].names) == {"to_id", "from_id"}
