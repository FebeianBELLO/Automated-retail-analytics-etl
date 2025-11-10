# ==========================================
# automate_pipeline.py
# Automated ETL pipeline for BusinessRetailDB
# ==========================================

import os
import sys
import hashlib
import subprocess
import urllib
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

# ------------------------------
# CONFIGURATION
# ------------------------------
BASE_PATH = r"C:\Users\Administrator\Downloads\Aaaaa_business_folder"
PRODUCT_FOLDER = os.path.join(BASE_PATH, "product_sales")
SUMMARY_FOLDER = os.path.join(BASE_PATH, "summary_sales")
DB_NAME = "BusinessRetailDB"
LOG_FILE = os.path.join(BASE_PATH, "pipeline_log.txt")
TASK_NAME = "BusinessDataRefreshTask"

# Ensure folders exist
os.makedirs(PRODUCT_FOLDER, exist_ok=True)
os.makedirs(SUMMARY_FOLDER, exist_ok=True)

# SQL SERVER CONNECTION
params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=DESKTOP-BRPE7R0\\SQLEXPRESS16;"
    "DATABASE=BusinessRetailDB;"
    "Trusted_Connection=yes;"
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# ------------------------------
# CLEANING FUNCTION
# ------------------------------
def clean_dataframe(df, is_product=True):
    """Standardize and clean DataFrame columns and values."""
    df.columns = [c.strip().replace(" ", "_") for c in df.columns]
    if is_product and "Product_Type" in df.columns:
        df["Product_Type"] = df["Product_Type"].fillna("Unknown")
    num_cols = df.select_dtypes(include=["number"]).columns
    df[num_cols] = df[num_cols].fillna(0)
    return df

# ------------------------------
# FILE HASH UTILITY
# ------------------------------
def get_file_hash(file_path):
    """Generate MD5 hash for file to detect duplicates."""
    with open(file_path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()

# ------------------------------
# LOAD FUNCTION
# ------------------------------
def load_folder_to_sql(folder_path, table_name, is_product=True):
    """Load CSVs from folder to SQL Server, avoiding duplicates."""
    print(f"\n Scanning folder: {folder_path}")
    all_files = [f for f in os.listdir(folder_path) if f.lower().endswith(".csv")]

    if not all_files:
        print(f" No CSV files found in {folder_path}. Skipping.")
        return 0

    # Hash tracking file
    hash_log_path = os.path.join(BASE_PATH, "loaded_files_hash.csv")
    if os.path.exists(hash_log_path):
        hash_df = pd.read_csv(hash_log_path)
    else:
        hash_df = pd.DataFrame(columns=["FolderType", "FileName", "FileHash", "Load_Timestamp"])

    loaded_rows = 0

    for file_name in all_files:
        file_path = os.path.join(folder_path, file_name)
        file_hash = get_file_hash(file_path)
        file_date = datetime.fromtimestamp(os.path.getmtime(file_path))
        folder_type = "Product" if is_product else "Summary"

        #  Duplicate check
        already_loaded = (
            (hash_df["FolderType"] == folder_type)
            & (hash_df["FileName"] == file_name)
            & (hash_df["FileHash"] == file_hash)
        ).any()

        if already_loaded:
            print(f" Skipping '{file_name}' — already loaded with same hash.")
            continue

        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            print(f" Error reading '{file_name}': {e}")
            continue

        if df.empty:
            print(f" '{file_name}' is empty. Skipping.")
            continue

        # Clean + add metadata
        df = clean_dataframe(df, is_product)
        df["Source_File"] = file_name
        df["Load_Timestamp"] = datetime.now()
        df["File_Drop_Time"] = file_date

        print(f"\n Preview of '{file_name}':")
        print(df.head(2))
        print(f" Rows: {len(df)}, Columns: {len(df.columns)}")

        # Append to SQL
        try:
            df.to_sql(table_name, engine, if_exists="append", index=False, method="multi", chunksize=500)
            loaded_rows += len(df)
            print(f" Loaded {len(df)} rows from '{file_name}' into '{table_name}'.")
        except Exception as e:
            print(f" SQL insert failed for '{file_name}': {e}")
            continue

        # Update hash log
        hash_df = pd.concat(
            [
                hash_df,
                pd.DataFrame(
                    [
                        {
                            "FolderType": folder_type,
                            "FileName": file_name,
                            "FileHash": file_hash,
                            "Load_Timestamp": datetime.now(),
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )

        # Log to file
        with open(LOG_FILE, "a", encoding="utf-8") as log:
            log.write(f"{datetime.now()}: Loaded '{file_name}' into '{table_name}' ({len(df)} rows)\n")

    # Save hash log
    hash_df.drop_duplicates(subset=["FolderType", "FileName", "FileHash"], keep="last", inplace=True)
    hash_df.to_csv(hash_log_path, index=False)

    print(f" Total new rows loaded into '{table_name}': {loaded_rows}")
    return loaded_rows

# ------------------------------
# MAIN PIPELINE
# ------------------------------
def process_and_load():
    print("\n Starting ETL process...\n")

    print(" Processing Product CSVs...")
    load_folder_to_sql(PRODUCT_FOLDER, "ProductSales", is_product=True)

    print("\n Processing Summary CSVs...")
    load_folder_to_sql(SUMMARY_FOLDER, "SummarySales", is_product=False)

    print("\n ETL process completed successfully.")
    print(f" {datetime.now()}\n")

# ------------------------------
# TASK SCHEDULER SETUP
# ------------------------------
def register_task_every_2_hours():
    """Registers the script to run every 2 hours automatically."""
    python_exe = sys.executable
    script_path = os.path.abspath(__file__)
    cmd = [
        "schtasks",
        "/Create",
        "/SC", "HOURLY",
        "/MO", "2",
        "/TN", TASK_NAME,
        "/TR", f'"{python_exe}" "{script_path}"',
        "/F",
    ]
    try:
        subprocess.run(" ".join(cmd), check=True, shell=True)
        print(f" Task '{TASK_NAME}' registered successfully (every 2 hours).")
    except subprocess.CalledProcessError as e:
        print(f" Failed to register scheduled task: {e}")

# ------------------------------
# EXECUTION
# ------------------------------
if __name__ == "__main__":
    run_now = True  # Change to False if you want to rely on scheduler only
    if run_now:
        print("Running pipeline immediately (manual override)...")
        process_and_load()
    else:
        print("Waiting for scheduled runs...")

# python .\automate_pipeline.py
