from datarush.config import DatarushConfig, get_datarush_config, set_datarush_config
from datarush.core.dataflow import Operation


def test_set_and_get_datarush_config():
    """Test setting and getting the Datarush configuration."""

    class CustomOperation(Operation):
        name: str = "custom_operation"
        title: str = "Custom Operation"

    def custom_s3_config_factory():
        return "custom s3 config"

    config = DatarushConfig(
        custom_operations=[CustomOperation], s3_config_factory=custom_s3_config_factory
    )
    set_datarush_config(config)
    retrieved_config = get_datarush_config()

    assert retrieved_config.custom_operations == [CustomOperation]
    assert retrieved_config.s3 == "custom s3 config"
    assert retrieved_config is config
