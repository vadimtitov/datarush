# Operation Model Types

This document lists all types used by instances of `BaseOperationModel` and explains what Streamlit element they render in the UI. Optionals of any of these types are not supported yet but will be added for some types in the future.

## Supported Types

| Type                                  | Description                           | Streamlit Element                            | Notes                                                              |
| ------------------------------------- | ------------------------------------- | -------------------------------------------- | ------------------------------------------------------------------ |
| `str`                                 | Basic string input                    | `st.text_input()`                            | Standard text input field                                          |
| `int`                                 | Integer input                         | `st.number_input(step=1)`                    | Number input with integer step                                     |
| `float`                               | Float input                           | `st.number_input(step=0.01)`                 | Number input with decimal step                                     |
| `bool`                                | Boolean input                         | `st.checkbox()`                              | Checkbox for true/false values                                     |
| `bytes`                               | File upload                           | `st.file_uploader()`                         | File upload widget (used for local file source)                    |
| `list[str]`                           | List of strings                       | `st.multiselect()`                           | Multi-select dropdown for string lists                             |
| `list[ColumnStr]`                     | List of column names                  | `st.multiselect()`                           | Multi-select dropdown with available columns                       |
| `list[TableStr]`                      | List of table names                   | `st.multiselect()`                           | Multi-select dropdown with available tables                        |
| `typing.Literal` (str)             | Enumeration of specific string values | `st.selectbox()`                             | Dropdown with predefined options                                   |
| `enum.StrEnum`                        | Enumeration of specific values        | `st.selectbox()`                             | Dropdown with options predefined in string enum                    |
| `TableStr`                            | Single table name                     | `st.selectbox()`                             | Dropdown with available tables                                     |
| `ColumnStr`                           | Single column name                    | `st.selectbox()`                             | Dropdown with available columns                                    |
| `TextStr`                             | Multi-line text input                 | `st.text_area()`                             | Large text area for multi-line content                             |
| `StringMap`                           | Dictionary of string key-value pairs  | `st.data_editor()`                           | Editable table with key-value columns                              |
| `RowCondition`                        | Single condition for filtering        | Custom container with multiple inputs        | Complex form with column, operator, value, type, and negate fields |
| `RowConditionGroup`                   | Group of conditions for filtering     | Custom container with dynamic condition list | Complex form with add/remove conditions and combine logic          |
| `PartitionFilterGroup`                | Group of partition filters            | Custom container with dynamic filter list    | Similar to RowConditionGroup but for partition filtering           |
| `Annotated[ColumnStr, ColumnStrMeta]` | Column with metadata                  | `st.selectbox()`                             | Same as ColumnStr but with table context metadata                  |
