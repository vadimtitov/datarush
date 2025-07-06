# DataRush Documentation

Welcome to the DataRush documentation! DataRush is a no-code data pipeline framework that allows you to build, execute, and manage data workflows through a visual interface or programmatic API.

## Quick Start

- **[Getting Started](docs/GettingStarted.md)** - Get up and running with DataRush in minutes
- **[Configuration](docs/Configuration.md)** - Learn about environment variables and settings

## Core Concepts

- **[Operations](docs/Operations.md)** - Complete guide to all available data operations
- **[Operation Model Types](docs/OperationModelTypes.md)** - Reference for supported field types and UI elements
- **[Templates](docs/Templates.md)** - How to create and use reusable data pipeline templates

## Advanced Topics

- **[Advanced Usage](docs/AdvancedUsage.md)** - Jinja2 templating, custom operations, and advanced features


## Development & Troubleshooting

- **[Contributing](docs/Contributing.md)** - How to contribute to DataRushNo
- **[Troubleshooting](docs/Troubleshooting.md)** - Common issues and solutions

## What is DataRush?

DataRush is a Python-based framework for creating data pipelines without writing code. It provides:

- **Visual Interface**: Web-based UI built with Streamlit for building pipelines
- **Template System**: Reusable, versioned pipeline configurations
- **Rich Operation Library**: 30+ built-in operations for data processing
- **Multiple Execution Modes**: UI, CLI, and programmatic execution
- **Cloud Integration**: Native S3 support for data sources and sinks

## Key Features

### Data Sources

- Local files (CSV, JSON, Parquet)
- S3 objects and datasets
- HTTP API requests

### Data Transformations

- Filtering, sorting, and grouping
- Joins and concatenations
- Data type conversions
- Aggregations and pivots
- Custom calculations and derivations

### Data Sinks

- S3 object and dataset writing
- Multiple output formats

### Advanced Features

- Jinja2 templating for dynamic values
- Custom operation development
- Parameterized templates
- Version control for pipelines
