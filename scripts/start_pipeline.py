"""
start_pipeline.py
-----------------
Entry point for the SQL Data Warehouse & Analytics Pipeline.
This script orchestrates the ETL process from raw data ingestion
to data transformation and loading into the warehouse.

Technologies: Python, SQL Server, pandas, pyodbc
"""

import os
import pandas as pd
import pyodbc
from datetime import datetime

# -------------------------------
# CONFIGURATION
# -------------------------------
DB_CONFIG = {
    "server": "localhost",       # SQL Server instance
    "database": "DataWarehouse", # Target database
    "username": "your_user",
    "password": "your_password",
    "driver": "{ODBC Driver 18 for SQL Server}"  # adjust version
}

DATA_DIR = "./datasets"         # Directory for raw CSVs
LOG_DIR = "./logs"              # Directory for ETL logs

# -------------------------------
# UTILITY FUNCTIONS
# -------------------------------

def create_connection(config):
    """
    Establish connection to SQL Server database.
    """
    conn_str = (
        f"DRIVER={config['driver']};"
        f"SERVER={config['server']};"
        f"DATABASE={config['database']};"
        f"UID={config['username']};"
        f"PWD={config['password']}"
    )
    conn = pyodbc.connect(conn_str)
    return conn

def log_message(message):
    """
    Simple logging function.
    """
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    log_file = os.path.join(LOG_DIR, "pipeline.log")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a") as f:
        f.write(f"[{timestamp}] {message}\n")
    print(message)

# -------------------------------
# ETL STAGES
# -------------------------------

def extract_data(file_path):
    """
    Extract data from CSV into pandas DataFrame.
    """
    try:
        df = pd.read_csv(file_path)
        log_message(f"Extracted data from {file_path} ({len(df)} rows)")
        return df
    except Exception as e:
        log_message(f"Error extracting {file_path}: {e}")
        return None

def transform_data(df):
    """
    Placeholder for data cleaning, standardization, and transformations.
    """
    # Example transformation: strip whitespace from column names
    df.columns = df.columns.str.strip()
    log_message("Transformed data")
    return df

def load_data(df, table_name, conn):
    """
    Load DataFrame into SQL Server table.
    """
    try:
        cursor = conn.cursor()
        # Replace with proper bulk insert/ETL code
        for index, row in df.iterrows():
            placeholders = ",".join(["?"] * len(row))
            sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
            cursor.execute(sql, tuple(row))
        conn.commit()
        log_message(f"Loaded data into {table_name} ({len(df)} rows)")
    except Exception as e:
        log_message(f"Error loading {table_name}: {e}")

# -------------------------------
# PIPELINE EXECUTION
# -------------------------------

def run_pipeline():
    """
    Orchestrates ETL pipeline.
    """
    conn = create_connection(DB_CONFIG)

    # Example: loop through all CSVs in dataset folder
    for file in os.listdir(DATA_DIR):
        if file.endswith(".csv"):
            file_path = os.path.join(DATA_DIR, file)
            df = extract_data(file_path)
            if df is not None:
                df_transformed = transform_data(df)
                table_name = os.path.splitext(file)[0]  # table name = file name
                load_data(df_transformed, table_name, conn)

    conn.close()
    log_message("ETL pipeline completed successfully!")

# -------------------------------
# MAIN
# -------------------------------
if __name__ == "__main__":
    run_pipeline()
