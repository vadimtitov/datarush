# Advanced Usage

This guide covers advanced features of DataRush, including Jinja2 templating, custom operations, advanced UI features, and performance optimization.

## Jinja2 Templating

DataRush supports Jinja2 templating for dynamic values in operations and templates. Note that advanced mode needs to be enabled for operation first

### Advanced Mode

Enable advanced mode for Jinja2 template editing:

1. **Toggle Advanced Mode**: Click the advanced mode button `{}` in operation forms
2. **Template Editor**: Use the Jinja2 template editor with syntax highlighting
3. **Live Preview**: See rendered values in real-time
4. **Error Detection**: Get immediate feedback on template syntax errors


### Basic Template Syntax
Refer to [Jinja2 docs](https://jinja.palletsprojects.com/en/stable/) for Jinja2 syntax

Example:
```json
{
  "name": "read_s3_object",
  "data": {
    "bucket": "{{ parameters.bucket }}",
    "object_key": "{{ parameters.date }}/{{ parameters.filename }}",
    "content_type": "{{ parameters.content_type | default('CSV') }}",
    "table_name": "{{ parameters.table_name }}"
  },
  "advanced_mode": true
}
```



### Available Context Variables

Currently, DataRush supports the `parameters` context for accessing template parameters:


## Custom Operations

Create your own operations to extend DataRush's functionality.

### Basic Custom Operation

```python
from datarush.core.dataflow import Operation, Tableset
from datarush.core.types import BaseOperationModel, TableStr
from pydantic import Field
import pandas as pd

class CustomFilterModel(BaseOperationModel):
    """Model for custom filter operation."""

    table: TableStr = Field(title="Table", description="Table to filter")
    threshold: float = Field(title="Threshold", description="Filtering threshold", default=0.5)

class CustomFilterOperation(Operation):
    """Custom operation to filter data based on a threshold."""

    name = "custom_filter"
    title = "Custom Filter"
    description = "Filter data using custom logic"
    model: CustomFilterModel

    def summary(self) -> str:
        """Provide operation summary."""
        return f"Custom filter on `{self.model.table}` with threshold {self.model.threshold}"

    def operate(self, tableset: Tableset) -> Tableset:
        """Run the custom filter operation."""
        df = tableset.get_df(self.model.table)

        # Custom filtering logic
        filtered_df = df[df['score'] > self.model.threshold]

        tableset.set_df(self.model.table, filtered_df)
        return tableset
```

### Registering Custom Operations

#### Via Configuration

```python
from datarush.config import DatarushConfig
from datarush.ui import run_ui
from my_custom_operations import CustomFilterOperation

def get_datarush_config():
    return DatarushConfig(
        custom_operations=[CustomFilterOperation]
    )

if __name__ == "__main__":
    run_ui(config_factory=get_datarush_config)
```

#### Via Programmatic Registration

```python
from datarush.core.operations import register_operation_type
from my_custom_operations import CustomFilterOperation

# Register the operation
register_operation_type(CustomFilterOperation)

# Now use in UI or templates
```
