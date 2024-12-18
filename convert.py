import pandas as pd
import os
import mysql.connector
from mysql.connector import Error

# Database configuration
config = {
    'user': 'root',
    'password': 'root@123',
    'host': 'localhost',
    'database': 'db_1367'
}

# Root directory containing your Parquet files
root_directory = '/Users/mohammedzughayer/Downloads/db_1367'

# Function to connect to the MySQL database
def create_connection(config):
    try:
        connection = mysql.connector.connect(**config)
        if connection.is_connected():
            db_info = connection.get_server_info()
            print(f"Successfully connected to MySQL Server version {db_info}")
        return connection
    except Error as e:
        print("Error while connecting to MySQL", e)
        return None

# Map pandas datatypes to MySQL datatypes
def dtype_mapper(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME"
    elif pd.api.types.is_string_dtype(dtype):
        return "TEXT"
    else:
        return "VARCHAR(255)"

# Function to generate and execute a SQL CREATE TABLE statement based on a DataFrame schema
def create_table_from_df(df, table_name, connection):
    cursor = connection.cursor()
    try:
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`;")
        columns = ", ".join([f"`{col}` {dtype_mapper(df[col].dtype)} DEFAULT NULL" for col in df.columns])
        create_table_query = f"CREATE TABLE `{table_name}` ({columns});"
        cursor.execute(create_table_query)
        connection.commit()
        print(f"Table `{table_name}` created successfully")
    except Error as e:
        print(f"Error while creating table {table_name}: {e}")
    finally:
        cursor.close()

# Function to write DataFrame to MySQL using bulk insert
def dataframe_to_sql(df, table_name, connection):
    cursor = connection.cursor()
    try:
        # Truncate data for 'transaction' column to 255 characters if it exists
        if 'transaction' in df.columns:
            df['transaction'] = df['transaction'].apply(lambda x: x[:255] if isinstance(x, str) else x)
        # Convert NaN to None in DataFrame directly
        df = df.astype(object).where(pd.notnull(df), None)
        data = [tuple(x) for x in df.to_numpy()]
        placeholders = ', '.join(['%s'] * len(df.columns))
        columns = ', '.join([f"`{column}`" for column in df.columns])
        sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
        cursor.executemany(sql, data)
        connection.commit()
        print("Data has been inserted successfully")
    except Error as e:
        print(f"Error inserting data into {table_name}: {e}")
    finally:
        cursor.close()


# Function to recursively find Parquet files, create tables, and load them into MySQL
def process_parquet_files(directory, connection):
    first_level_directories = next(os.walk(directory))[1]  # List first-level directories
    for dir_name in first_level_directories:
        full_dir_path = os.path.join(directory, dir_name)
        table_name = full_dir_path.split('/')[-1].replace('db_1367.', '')  # Extract table name from path
        for root, dirs, files in os.walk(full_dir_path):
            all_data = pd.DataFrame()  # Initialize an empty DataFrame
            for file in files:
                if file.endswith('.parquet'):
                    file_path = os.path.join(root, file)
                    df = pd.read_parquet(file_path)
                    all_data = pd.concat([all_data, df], ignore_index=True)
            if not all_data.empty:
                create_table_from_df(all_data, table_name, connection)
                dataframe_to_sql(all_data, table_name, connection)

# Connect to MySQL
connection = create_connection(config)
if connection is not None:
    process_parquet_files(root_directory, connection)
    connection.close()
    print("MySQL connection is closed")
