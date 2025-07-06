# Getting Started with DataRush

This guide will help you get up and running with DataRush quickly. You'll learn how to install DataRush, set up your environment, and create your first data pipeline.

## Installation

### Prerequisites

- Python 3.11 or higher
- pip (Python package installer)

### Install DataRush

```bash
pip install datarush
```

## Quick Setup

### 1. Configuration

DataRush uses environment variables for configuration. Create a `.env` file in your project directory:

```bash
# Template Store Configuration
TEMPLATE_STORE_TYPE=FILESYSTEM
TEMPLATE_STORE_FILESYSTEM_PATH=./templates

# S3 Configuration (optional - for cloud storage)
S3_ENDPOINT=your-s3-endpoint
S3_ACCESS_KEY=your-access-key
S3_SECRET_KEY=your-secret-key
S3_DEFAULT_BUCKET=your-default-bucket
```


Note: You can also configure DataRush programmatically via `datarush.config.DatarushConfig` object (more in Configuration section)


### 2. Create UI Entry Point

Create a `run_ui.py` file to launch the DataRush interface:

```python
from datarush.ui import run_ui

if __name__ == "__main__":
    run_ui()
```

### 3. Launch the UI

```bash
streamlit run run_ui.py
```

The DataRush interface will open in your browser at `http://localhost:8501`.

## Your First Pipeline

Let's create a simple data pipeline that reads a CSV file, filters some data, and saves the results.

### Step 1: Prepare Your Data

Create a sample CSV file `sample_data.csv`:

```csv
name,age,city,salary
Alice,25,New York,50000
Bob,30,San Francisco,75000
Charlie,35,Chicago,60000
Diana,28,Boston,65000
Eve,32,Seattle,80000
```

### Step 2: Build the Pipeline

1. **Open DataRush UI** and navigate to the Operations page
2. **Add a Local File Source**:
   - Choose "Local File" operation
   - Set Content Type to "CSV"
   - Upload your `sample_data.csv` file
   - Set Table Name to "employees"
3. **Add a Filter Operation**:
   - Choose "Filter Rows" operation
   - Select table "employees"
   - Add condition: `salary > 60000`
4. **Add a Sort Operation**:
   - Choose "Sort" operation
   - Select table "employees"
   - Sort by "salary" in descending order
5. **Add an S3 Sink** (or use local file for testing):
   - Choose "Write S3 Object" operation
   - Set bucket and object key
   - Set Content Type to "CSV"

### Step 3: Run the Pipeline

Click "Run Operations" to execute your pipeline. The filtered and sorted data will be saved to your specified destination.

## Using Templates

Templates allow you to save and reuse pipelines with different parameters.

### Creating a Template

1. Build your pipeline in the UI
2. Click the "Save" button in the UI
3. Enter a template name and version when prompted
4. The template will be automatically saved to your configured template store

### Running Templates

#### Via CLI

```bash
python -m datarush --template "my_template" --version "1.0.0"
```

#### Via Python API

```python
from datarush import run_template

run_template(
    name="my_template",
    version="1.0.0",
    parameters={"input_file": "data.csv"}
)
```

## Common Use Cases

### ETL Pipeline

```python
# Example: Extract from API, Transform, Load to S3
from datarush import run_template

run_template(
    name="api_to_s3_etl",
    version="1.0.0",
    parameters={
        "api_url": "https://api.example.com/data",
        "output_bucket": "my-data-bucket",
        "output_key": "processed_data.csv"
    }
)
```

### Data Cleaning

```python
# Example: Clean and validate data
run_template(
    name="data_cleaning",
    version="1.0.0",
    parameters={
        "input_file": "raw_data.csv",
        "remove_duplicates": True,
        "fill_missing": "mean"
    }
)
```

## Next Steps

- **[Operations Guide](Operations.md)** - Learn about all available operations
- **[Configuration](Configuration.md)** - Advanced configuration options
- **[Templates](Templates.md)** - Creating and managing templates
- **[Advanced Usage](AdvancedUsage.md)** - Jinja2 templating and custom operations
