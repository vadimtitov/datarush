from io import BytesIO

import pandas as pd

from datarush.core.types import ContentType


def read_file(file: BytesIO, content_type: ContentType) -> pd.DataFrame:
    if content_type == ContentType.CSV:
        return pd.read_csv(file)
    elif content_type == ContentType.JSON:
        return pd.read_json(file)
    elif content_type == ContentType.PARQUET:
        return pd.read_parquet(file)
    else:
        raise ValueError(f"Unsupported content type: {content_type}")


def truncate(text: str, n: int) -> str:
    return text if len(text) <= n else text[: n - 3] + "..."
