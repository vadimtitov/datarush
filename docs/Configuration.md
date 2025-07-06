# DataRush Configuration

This guide covers all configuration options for DataRush, including environment variables, programmatic configuration, and advanced settings.

## Overview

DataRush can be configured in two ways:

- **Environment Variables**: Using `.env` files or system environment variables
- **Programmatic Configuration**: Passing `DatarushConfig` objects directly to functions

## Environment Variables

### Template Store Configuration

| Variable                         | Description                                                 | Default    | Required      |
| -------------------------------- | ----------------------------------------------------------- | ---------- | ------------- |
| `TEMPLATE_STORE_TYPE`            | Template storage type: `S3` or `FILESYSTEM`                 | -          | Yes           |
| `TEMPLATE_STORE_S3_BUCKET`       | S3 bucket for templates (when using S3 store)               | -          | If S3         |
| `TEMPLATE_STORE_S3_PREFIX`       | S3 prefix for templates (when using S3 store)               | `datarush` | No            |
| `TEMPLATE_STORE_FILESYSTEM_PATH` | Filesystem path for templates (when using filesystem store) | `.`        | If FILESYSTEM |

### S3 Configuration

| Variable            | Description                               | Default | Required                |
| ------------------- | ----------------------------------------- | ------- | ----------------------- |
| `S3_ENDPOINT`       | S3 service endpoint URL                   | -       | Yes (for S3 operations) |
| `S3_ACCESS_KEY`     | S3 access key                             | -       | Yes (for S3 operations) |
| `S3_SECRET_KEY`     | S3 secret key                             | -       | Yes (for S3 operations) |
| `S3_DEFAULT_BUCKET` | Default bucket suggested in operations UI | -       | No                      |

## Configuration Examples

### Basic Filesystem Setup

```bash
# .env file
TEMPLATE_STORE_TYPE=FILESYSTEM
TEMPLATE_STORE_FILESYSTEM_PATH=./templates
```

### S3 Setup

```bash
# .env file
TEMPLATE_STORE_TYPE=S3
TEMPLATE_STORE_S3_BUCKET=my-datarush-templates
TEMPLATE_STORE_S3_PREFIX=datarush/templates

S3_ENDPOINT=https://s3.amazonaws.com
S3_ACCESS_KEY=your-access-key
S3_SECRET_KEY=your-secret-key
S3_DEFAULT_BUCKET=my-data-bucket
```


## Programmatic Configuration

### DatarushConfig Class

The `DatarushConfig` class allows you to configure DataRush programmatically. It currently supports custom operations and S3 configuration overrides:

```python
from datarush.config import DatarushConfig, S3Config

config = DatarushConfig(
    custom_operations=[MyCustomOperation],
    s3_config_factory=lambda: S3Config()
)
```

### Parameters

- `custom_operations` (list[type[Operation]] | None): List of custom operations to register
- `s3_config_factory` (Callable[[], S3Config] | None): Optional factory function to create custom S3 configuration

### Using Configuration with Different Entry Points

#### UI Configuration

```python
from functools import cache
from datarush.config import DatarushConfig
from datarush.ui import run_ui
from my_custom_operations import MyCustomOperation

def get_config():
    return DatarushConfig(
        custom_operations=[MyCustomOperation]
    )

if __name__ == "__main__":
    run_ui(config_factory=get_config)
```

#### Template Execution Configuration

```python
from datarush.config import DatarushConfig
from datarush import run_template

config = DatarushConfig(
    custom_operations=[MyCustomOperation]
)

run_template(
    name="my_template",
    version="1.0.0",
    config=config,
    parameters={"input_file": "data.csv"}
)
```

#### CLI Configuration

```python
from datarush.config import DatarushConfig
from datarush import run_template_from_command_line

config = DatarushConfig(
    custom_operations=[MyCustomOperation]
)

if __name__ == "__main__":
    run_template_from_command_line(config=config)
```

### Custom S3 Configuration

You can provide a custom S3 configuration factory:

```python
from datarush.config import DatarushConfig, S3Config

def create_custom_s3_config():
    return S3Config(
        endpoint="https://custom-s3-endpoint.com",
        access_key="custom-key",
        secret_key="custom-secret"
    )

config = DatarushConfig(
    s3_config_factory=create_custom_s3_config
)
```
