"""
Run datarush UI.

Exported into a separate script because
streamlit command only accepts .py file as argument
"""

from datarush.config import DatarushConfig, S3Config
from datarush.ui import run_ui


def _get_s3_config() -> S3Config:
    """Get S3 configuration."""
    return S3Config.fromenv()


if __name__ == "__main__":
    run_ui(
        config=DatarushConfig(
            custom_operations=[],
            s3_config_factory=_get_s3_config,
        )
    )
