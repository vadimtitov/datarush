# DataRush Templates

Templates are reusable, versioned pipeline configurations that allow you to save and execute data workflows with different parameters.

## Overview

Templates in DataRush are JSON files that contain:

- **Parameters**: Input variables that can be customized per execution
- **Operations**: The sequence of data processing steps
- **Metadata**: Name, version, and description information

## Creating Templates

### Via UI

1. **Build your pipeline** in the DataRush UI
2. **Click "Save"** button in the interface
3. **Enter template details**:
   - Template name (e.g., "data_cleaning")
   - Version (e.g., "1.0.0")
4. **Add parameters** (optional):
   - Define input parameters that can be customized
   - Set default values and descriptions

### Template Structure

A DataRush template has this structure:

```json
{
    "parameters": [
        {
            "name": "input_file",
            "type": "string",
            "description": "Input CSV file path",
            "default": "data.csv",
            "required": true
        },
        {
            "name": "output_bucket",
            "type": "string",
            "description": "S3 bucket for output",
            "default": "my-data-bucket",
            "required": true
        }
    ],
    "operations": [
        {
            "name": "read_s3_object",
            "data": {
                "bucket": "{{ parameters.input_bucket }}",
                "object_key": "{{ parameters.input_file }}",
                "content_type": "CSV",
                "table_name": "raw_data"
            },
            "advanced_mode": true
        },
        {
            "name": "filter_rows",
            "data": {
                "table": "raw_data",
                "conditions": {
                    "combine": "and",
                    "conditions": [
                        {
                            "column": "status",
                            "operator": "equals",
                            "value": "active",
                            "value_type": "string"
                        }
                    ]
                }
            },
            "advanced_mode": false
        },
        {
            "name": "write_s3_object",
            "data": {
                "bucket": "{{ parameters.output_bucket }}",
                "object_key": "cleaned_data.csv",
                "content_type": "CSV",
                "table": "raw_data"
            },
            "advanced_mode": true
        }
    ],
    "datarush_version": "0.8.0"
}
```

## Template Parameters

Parameters allow you to customize template execution without modifying the template itself.

### Parameter Types

| Type       | Description       | Example                 |
| ---------- | ----------------- | ----------------------- |
| `string`   | Text values       | File paths, URLs, names |
| `integer`  | Whole numbers     | Row counts, timeouts    |
| `float`    | Decimal numbers   | Thresholds, ratios      |
| `date`     | Date values       | Start dates, end dates  |
| `datetime` | Date and time     | Timestamps              |
| `boolean`  | True/false values | Flags, options          |

### Parameter Examples

```json
{
  "parameters": [
    {
      "name": "input_file",
      "type": "string",
      "description": "Input file path",
      "default": "data.csv",
      "required": true
    },
    {
      "name": "threshold",
      "type": "float",
      "description": "Filtering threshold",
      "default": "0.5",
      "required": false
    },
    {
      "name": "enable_logging",
      "type": "boolean",
      "description": "Enable detailed logging",
      "default": "true",
      "required": false
    }
  ]
}
```

## Template Versioning

Templates version can be any string but it is recommended to use semantic versioning (MAJOR.MINOR.PATCH):

- **MAJOR**: Breaking changes (incompatible parameters or operations)
- **MINOR**: New features (new parameters, operations)
- **PATCH**: Bug fixes (no functional changes)

## Executing Templates

### Command Line

```bash
# Basic execution
python -m datarush --template "data_cleaning" --version "1.0.0"

# With parameters
python -m datarush \
  --template "data_cleaning" \
  --version "1.0.0" \
  --input_file "new_data.csv" \
  --output_bucket "my-bucket"
```

### Python API

```python
from datarush import run_template

# Basic execution
run_template(
    name="data_cleaning",
    version="1.0.0"
)

# With parameters
run_template(
    name="data_cleaning",
    version="1.0.0",
    parameters={
        "input_file": "new_data.csv",
        "output_bucket": "my-bucket",
        "threshold": 0.8
    }
)
```

### Programmatic Configuration

```python
from datarush import run_template
from datarush.config import DatarushConfig

config = DatarushConfig(
    custom_operations=[MyCustomOperation]
)

run_template(
    name="data_cleaning",
    version="1.0.0",
    config=config,
    parameters={"input_file": "data.csv"}
)
```