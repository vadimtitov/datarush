import os

import pytest
from envarify import SecretString

from datarush.config import DatarushConfig, S3Config, set_datarush_config


@pytest.fixture(autouse=True)
def setup_config():
    set_datarush_config(
        DatarushConfig(
            custom_operations=[],
            s3_config_factory=lambda: S3Config(
                endpoint="http://example.com",
                access_key="mock_access_key",
                secret_key=SecretString("mock_secret_key"),
            ),
        ),
    )
    os.environ["TEMPLATE_STORE_TYPE"] = "FILESYSTEM"
    os.environ["TEMPLATE_STORE_S3_BUCKET"] = "sample-bucket"
    os.environ["TEMPLATE_STORE_S3_PREFIX"] = "datarush"
    os.environ["TEMPLATE_STORE_FILESYSTEM_PATH"] = "."
