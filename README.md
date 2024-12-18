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

The script uses the following database configuration:



## Features

- **Automatic Table Creation**: Creates MySQL tables based on Parquet file schemas
- **Data Type Mapping**: Automatically maps Pandas datatypes to appropriate MySQL datatypes
- **Bulk Data Insert**: Efficiently inserts data using bulk insert operations
- **Error Handling**: Includes comprehensive error handling for database operations
- **Directory Processing**: Recursively processes Parquet files from nested directories

## Main Functions

1. `create_connection(config)`: Establishes connection to MySQL database
2. `dtype_mapper(dtype)`: Maps Pandas datatypes to MySQL datatypes
3. `create_table_from_df(df, table_name, connection)`: Creates MySQL tables based on DataFrame schema
4. `dataframe_to_sql(df, table_name, connection)`: Inserts DataFrame data into MySQL tables
5. `process_parquet_files(directory, connection)`: Recursively processes Parquet files and loads them into MySQL

## Directory Structure

The script expects Parquet files to be organized in a directory structure where:
- Root directory contains subdirectories
- Each subdirectory name becomes the table name in MySQL
- Parquet files within these subdirectories contain the data to be loaded

## Usage

1. Update the database configuration in the script to match your MySQL settings
2. Set the `root_directory` variable to point to your Parquet files location
3. Run the script:


## Data Handling Notes

- The script automatically handles NULL values
- String data in 'transaction' columns is truncated to 255 characters
- Tables are dropped and recreated if they already exist
- All data from multiple Parquet files in the same directory is concatenated into a single table

## Error Handling

The script includes error handling for:
- Database connection issues
- Table creation errors
- Data insertion problems

## Limitations

- String fields are mapped to TEXT or VARCHAR(255)
- Existing tables with the same name will be dropped
- All Parquet files in a directory must have the same schema

## License

[Add your license information here]
