# DataRush Operations Guide

This guide covers all available operations in DataRush, organized by category. Each operation includes parameters, examples, and common use cases.

## Table of Contents

- [Data Sources](#data-sources)
  - [Local File Source](#local-file-source)
  - [S3 Object Source](#s3-object-source)
  - [S3 Dataset Source](#s3-dataset-source)
  - [HTTP Request Source](#http-request-source)
- [Data Transformations](#data-transformations)
  - [Basic Operations](#basic-operations)
    - [Filter Rows](#filter-rows)
    - [Select Columns](#select-columns)
    - [Sort](#sort)
    - [Rename Table](#rename-table)
  - [Data Cleaning](#data-cleaning)
    - [Drop NA Values](#drop-na-values)
    - [Fill NA Values](#fill-na-values)
    - [Normalize Empty Values](#normalize-empty-values)
    - [Deduplicate Rows](#deduplicate-rows)
    - [Deduplicate Column Values](#deduplicate-column-values)
    - [Replace](#replace)
  - [Data Type Operations](#data-type-operations)
    - [As Type](#as-type)
    - [Set Header](#set-header)
    - [Unset Header](#unset-header)
  - [Data Manipulation](#data-manipulation)
    - [Join Tables](#join-tables)
    - [Group By](#group-by)
    - [Pivot Table](#pivot-table)
  - [Advanced Transformations](#advanced-transformations)
    - [Calculate](#calculate)
    - [Derive Column](#derive-column)
    - [Parse JSON Column](#parse-json-column)
    - [Explode](#explode)
    - [Wide to Long](#wide-to-long)
- [Data Sinks](#data-sinks)
  - [S3 Object Sink](#s3-object-sink)
  - [S3 Dataset Sink](#s3-dataset-sink)

## Data Sources

Operations that read data from various sources into DataRush tables.

### Local File Source

**Operation**: `Local File`  
**Description**: Load data from a local file into a table.

**Parameters**:

- `content_type` (ContentType): File format (CSV, JSON, PARQUET)
- `file` (bytes): File content
- `table_name` (str): Name for the resulting table

**Example**:

```json
{
  "content_type": "CSV",
  "file": "<file_bytes>",
  "table_name": "employees"
}
```

---

### S3 Object Source

**Operation**: `Read S3 Object`  
**Description**: Load data from an S3 object into a table.

**Parameters**:

- `bucket` (str): S3 bucket name
- `object_key` (str): S3 object key
- `content_type` (ContentType): File format
- `table_name` (str): Name for the resulting table

**Example**:

```json
{
  "bucket": "my-data-bucket",
  "object_key": "raw/employees.csv",
  "content_type": "CSV",
  "table_name": "employees"
}
```

---

### S3 Dataset Source

**Operation**: `Read S3 Dataset`  
**Description**: Load data from an S3 dataset (partitioned data).

**Parameters**:

- `bucket` (str): S3 bucket name
- `prefix` (str): Dataset prefix
- `content_type` (ContentType): File format
- `table_name` (str): Name for the resulting table
- `partition_filters` (PartitionFilterGroup): Optional partition filtering

---

### HTTP Request Source

**Operation**: `Send HTTP Request`  
**Description**: Make an HTTP request and parse the response into a table.

**Parameters**:

- `method` (Literal): HTTP method (GET, POST, PUT, DELETE)
- `url` (str): Target URL
- `headers` (StringMap): Request headers
- `params` (StringMap): Query parameters
- `body` (TextStr): Request body (for POST/PUT)
- `response_format` (Literal): Response parsing (json, raw)
- `output_table` (str): Name for the resulting table

**Example**:

```json
{
  "method": "GET",
  "url": "https://api.example.com/users",
  "headers": { "Authorization": "Bearer token" },
  "response_format": "json",
  "output_table": "api_users"
}
```

---

## Data Transformations

Operations that modify, filter, and transform data.

### Basic Operations

#### Filter Rows

**Operation**: `Filter Rows`  
**Description**: Filter table rows based on column conditions.

**Parameters**:

- `table` (TableStr): Table to filter
- `conditions` (RowConditionGroup): Filter conditions

**Example**:

```json
{
  "table": "employees",
  "conditions": {
    "combine": "and",
    "conditions": [
      {
        "column": "salary",
        "operator": "is greater than",
        "value": "50000",
        "value_type": "integer"
      },
      {
        "column": "department",
        "operator": "equals",
        "value": "Engineering",
        "value_type": "string"
      }
    ]
  }
}
```

---

#### Select Columns

**Operation**: `Select Columns`  
**Description**: Keep only specified columns from a table.

**Parameters**:

- `table` (TableStr): Table to modify
- `columns` (list[ColumnStr]): Columns to keep

**Example**:

```json
{
  "table": "employees",
  "columns": ["name", "salary", "department"]
}
```

---

#### Sort

**Operation**: `Sort`  
**Description**: Sort table by specified column.

**Parameters**:

- `table` (TableStr): Table to sort
- `column` (ColumnStr): Column to sort by
- `ascending` (bool): Sort order (default: true)

---

#### Rename Table

**Operation**: `Rename Table`  
**Description**: Change the name of a table.

**Parameters**:

- `table` (TableStr): Current table name
- `new_name` (str): New table name

---

#### Rename Columns

**Operation**: `Rename Columns`  
**Description**: Rename columns using a mapping of old names to new names.

**Parameters**:

- `table` (TableStr): Table to modify
- `column_mapping` (StringMap): Map of column names to rename: {old_column_name: new_column_name}

**Example**:

```json
{
  "table": "employees",
  "column_mapping": {
    "emp_id": "employee_id",
    "dept": "department",
    "sal": "salary"
  }
}
```

---

### Data Cleaning

#### Drop NA Values

**Operation**: `Drop NA Values`  
**Description**: Remove rows with missing values.

**Parameters**:

- `table` (TableStr): Table to clean

---

#### Fill NA Values

**Operation**: `Fill NA`  
**Description**: Fill missing values in selected columns using various methods.

**Parameters**:

- `table` (TableStr): Table to modify
- `columns` (list[ColumnStr]): Columns to fill (empty = all columns)
- `method` (Literal): Fill method (ffill, bfill, mean, median, mode, constant)
- `value` (str): Fill value when method is 'constant'
- `limit` (int): Maximum consecutive fills for ffill/bfill
- `axis` (Literal): Fill direction (rows, columns)

**Example**:

```json
{
  "table": "employees",
  "columns": ["salary", "department"],
  "method": "constant",
  "value": "0",
  "axis": "rows"
}
```

---

#### Normalize Empty Values

**Operation**: `Normalize Empty Values`  
**Description**: Convert all empty-like values to null for consistency.

**Parameters**:

- `table` (TableStr): Table to modify
- `columns` (list[ColumnStr]): Columns to normalize (empty = all columns)
- `custom_empty_values` (list[str]): Additional strings to treat as empty

**Example**:

```json
{
  "table": "employees",
  "columns": ["department", "notes"],
  "custom_empty_values": ["N.A.", "NA", "Unknown", " "]
}
```

---

#### Deduplicate Rows

**Operation**: `Deduplicate Rows`  
**Description**: Remove duplicate rows based on specified columns.

**Parameters**:

- `table` (TableStr): Table to deduplicate
- `columns` (list[ColumnStr]): Columns to consider for duplicates
- `keep` (Literal): Which duplicate to keep (first, last, none)

---

#### Deduplicate Column Values

**Operation**: `Deduplicate Column Values`  
**Description**: Remove duplicates from list-like values inside each cell of a column.

**Parameters**:

- `table` (TableStr): Table to modify
- `column` (ColumnStr): Column containing list-like values to deduplicate

**Example**:

```json
{
  "table": "employees",
  "column": "skills"
}
```

---

#### Replace

**Operation**: `Replace`  
**Description**: Replace values in specified columns.

**Parameters**:

- `table` (TableStr): Table to modify
- `columns` (list[ColumnStr]): Columns to replace (empty = all columns)
- `to_replace` (StringMap): Map of old values to new values
- `regex` (bool): Treat keys as regular expressions

**Example**:

```json
{
  "table": "employees",
  "columns": ["department"],
  "to_replace": {
    "IT": "Engineering",
    "HR": "Human Resources"
  },
  "regex": false
}
```

---

### Data Type Operations

#### As Type

**Operation**: `As Type`  
**Description**: Convert column data type.

**Parameters**:

- `table` (TableStr): Table to modify
- `column` (ColumnStr): Column to convert
- `dtype` (Literal): Target type (int, float, string, boolean, category)
- `errors` (Literal): Error handling (raise, ignore)

---

#### Set Header

**Operation**: `Set Header`  
**Description**: Use a row as column headers.

**Parameters**:

- `table` (TableStr): Table to modify
- `row_index` (int): Row index to use as header (0-based)

---

#### Unset Header

**Operation**: `Unset Header`  
**Description**: Remove header row from tables.

**Parameters**:

- `tables` (list[TableStr]): Tables to modify

---

### Data Manipulation

#### Join Tables

**Operation**: `Join Tables`  
**Description**: Join two tables on specified columns.

**Parameters**:

- `left_table` (TableStr): Left table for join
- `right_table` (TableStr): Right table for join
- `left_on` (ColumnStr): Column in left table
- `right_on` (ColumnStr): Column in right table
- `join_type` (Literal): Join type (inner, left, right, outer)
- `output_table` (str): Name for resulting table

---

#### Group By

**Operation**: `Group By`  
**Description**: Group table by columns and apply aggregation.

**Parameters**:

- `table` (TableStr): Table to group
- `group_by` (list[ColumnStr]): Columns to group by
- `aggregation_column` (ColumnStr): Column to aggregate
- `agg_func` (Literal): Aggregation function (sum, mean, min, max, count)
- `output_table` (str): Name for resulting table

---

#### Pivot Table

**Operation**: `Pivot Table`  
**Description**: Create pivot table from DataFrame.

**Parameters**:

- `table` (TableStr): Table to pivot
- `index` (list[ColumnStr]): Columns to group by
- `columns` (list[ColumnStr]): Columns to spread across
- `values` (list[ColumnStr]): Columns to aggregate
- `aggfunc` (Literal): Aggregation function
- `output_table` (str): Name for resulting table

---

### Advanced Transformations

#### Calculate

**Operation**: `Calculate`  
**Description**: Apply mathematical expression to columns.

**Parameters**:

- `table` (TableStr): Table to modify
- `target_column` (str): Name for new column
- `expression` (str): Math expression using existing columns

**Example**:

```json
{
  "table": "employees",
  "target_column": "bonus",
  "expression": "salary * 0.1"
}
```

---

#### Derive Column

**Operation**: `Derive Column`  
**Description**: Create new column using Jinja2 template.

**Parameters**:

- `table` (TableStr): Table to modify
- `target_column` (str): Name for new column
- `template` (str): Jinja2 template using row fields

**Example**:

```json
{
  "table": "employees",
  "target_column": "full_name",
  "template": "{{ first_name }} {{ last_name }}"
}
```

---

#### Parse JSON Column

**Operation**: `Parse JSON Column`  
**Description**: Convert JSON strings to Python objects.

**Parameters**:

- `table` (TableStr): Table to modify
- `column` (ColumnStr): Column containing JSON strings
- `on_error` (Literal): Error handling (null, error)

---

#### Explode

**Operation**: `Explode Columns`  
**Description**: Expand list-like columns into multiple rows.

**Parameters**:

- `table` (TableStr): Table to modify
- `columns` (list[ColumnStr]): Columns to explode together

---

#### Wide to Long

**Operation**: `Wide to Long`  
**Description**: Reshape table from wide to long format.

**Parameters**:

- `table` (TableStr): Table to reshape
- `index_columns` (list[ColumnStr]): Columns to keep fixed
- `value_column` (str): Name for value column
- `variable_column` (str): Name for variable column
- `stubs` (list[str]): Stubname prefixes for wide columns

---

## Data Sinks

Operations that write data to various destinations.

### S3 Object Sink

**Operation**: `Write S3 Object`  
**Description**: Write table to S3 object.

**Parameters**:

- `bucket` (str): S3 bucket name
- `object_key` (str): S3 object key
- `content_type` (ContentType): Output format
- `table` (TableStr): Table to write

---

### S3 Dataset Sink

**Operation**: `Write S3 Dataset`  
**Description**: Write table to S3 dataset (partitioned storage).

**Parameters**:

- `bucket` (str): S3 bucket name
- `prefix` (str): Dataset prefix
- `content_type` (ContentType): Output format
- `table` (TableStr): Table to write
- `partition_columns` (list[ColumnStr]): Columns to partition by
- `unique_ids` (list[ColumnStr]): Columns for unique identification

---

