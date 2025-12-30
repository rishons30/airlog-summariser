import pandas as pd
from pathlib import Path

# Path to incoming CSV files
DATA_DIR = Path("data/incoming-logs")

def ingest_daily_logs():
    all_files = list(DATA_DIR.glob("OPS_*.csv"))

    if not all_files:
        print("No OPS CSV files found.")
        return None

    print(f"Found {len(all_files)} daily ops files")

    dataframes = []

    for file in all_files:
        print(f"Ingesting file: {file.name}")
        df = pd.read_csv(file)
        df["source_file"] = file.name  # track origin
        dataframes.append(df)

    combined_df = pd.concat(dataframes, ignore_index=True)

    print("\nIngestion Summary")
    print("-" * 30)
    print(f"Total records ingested: {len(combined_df)}")
    print(f"Unique aircraft: {combined_df['aircraft_id'].nunique()}")
    print(f"Date range: {combined_df['log_date'].min()} to {combined_df['log_date'].max()}")

    return combined_df


if __name__ == "__main__":
    ingest_daily_logs()
