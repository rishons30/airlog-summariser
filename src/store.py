import sqlite3
from pathlib import Path
import pandas as pd

DB_PATH = Path("data/aviation_logs.db")


def store_logs(df: pd.DataFrame):
    """
    Store cleaned aviation logs into SQLite database
    """

    # Ensure data directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)

    df.to_sql(
        name="aircraft_ops_logs",
        con=conn,
        if_exists="append",
        index=False
    )

    conn.close()

    print(f"Stored {len(df)} records into database")


if __name__ == "__main__":
    from ingest import ingest_daily_logs
    from clean import clean_logs

    raw_df = ingest_daily_logs()

    if raw_df is not None:
        cleaned_df = clean_logs(raw_df)
        store_logs(cleaned_df)
