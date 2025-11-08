# Parquet to MySQL Converter

This Python script converts Parquet files into MySQL tables. It recursively processes Parquet files from a directory structure and loads them into a MySQL database while maintaining the data structure and types.

## Prerequisites

- Python 3.x
- Required Python packages:
  - `pandas`
  - `mysql-connector-python`
- MySQL Server installed and running
- Parquet files organized in directories

## Configuration

The script requires the following command line arguments:

- `--user`: MySQL username
- `--password`: MySQL password 
- `--host`: MySQL host address
- `--db`: Database name
- `--src_path`: Root directory containing Parquet files

## Features

- **Automatic Table Creation**: Creates MySQL tables based on Parquet file schemas
- **Data Type Mapping**: Automatically maps Pandas datatypes to appropriate MySQL datatypes
- **Bulk Data Insert**: Efficiently inserts data using bulk insert operations
- **Error Handling**: Includes comprehensive error handling for database operations
- **Directory Processing**: Recursively processes Parquet files from nested directories
- **Command Line Arguments**: Configurable via command line for secure credential handling

## Main Functions

1. `get_required_arg(arg_name)`: Gets required command line arguments
2. `create_connection(config)`: Establishes connection to MySQL database
3. `dtype_mapper(dtype)`: Maps Pandas datatypes to MySQL datatypes
4. `create_table_from_df(df, table_name, connection)`: Creates MySQL tables based on DataFrame schema
5. `dataframe_to_sql(df, table_name, connection)`: Inserts DataFrame data into MySQL tables
6. `process_parquet_files(directory, connection)`: Recursively processes Parquet files and loads them into MySQL

## Directory Structure

The script expects Parquet files to be organized in a directory structure where:
- Root directory contains subdirectories
- Each subdirectory name becomes the table name in MySQL
- Parquet files within these subdirectories contain the data to be loaded

## Usage

Run the script with required arguments:
