"""
OTT ANALYTICS – SQL ANALYSIS (SQLite)
Simulates BigQuery analytics locally
Run: python scripts/sql_analytics.py
"""

import sqlite3
import pandas as pd
import json
import os
from glob import glob

# ================= PATHS =================
BASE_DIR = r"C:\Users\abbay\data\processed"
SUMMARY_DIR = os.path.join(BASE_DIR, "summary")
RAW_FILE = r"C:\Users\abbay\view_log_2025-12-20.csv"
DB_FILE = os.path.join(BASE_DIR, "ott_analytics.db")


# ================= LOAD SUMMARY DATA =================
def load_summary_data():
    files = glob(os.path.join(SUMMARY_DIR, "viewership_summary-*.json"))

    if not files:
        raise FileNotFoundError("No summary JSON shards found")

    records = []
    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))

    if not records:
        raise ValueError("Summary DataFrame is empty")

    df = pd.DataFrame(records)
    print(f"[OK] Loaded summary records: {len(df)}")
    print(f"[OK] Columns: {list(df.columns)}")

    return df


# ================= CREATE DATABASE =================
def create_database():
    print("Creating analytics database...")

    conn = sqlite3.connect(DB_FILE)

    summary_df = load_summary_data()
    summary_df.to_sql(
        "viewership_summary",
        conn,
        if_exists="replace",
        index=False
    )

    if os.path.exists(RAW_FILE):
        raw_df = pd.read_csv(RAW_FILE)
        raw_df.to_sql("raw_logs", conn, if_exists="replace", index=False)
        print(f"[OK] Raw logs loaded: {len(raw_df)} rows")

    print(f"[OK] SQLite DB created at: {DB_FILE}")
    return conn


# ================= RUN ANALYTICS =================
def run_queries(conn):
    queries = {
        "Top Shows by Watch Time": """
            SELECT
                show_id,
                SUM(total_watch_time) AS total_minutes,
                ROUND(SUM(total_watch_time)/60.0, 2) AS total_hours,
                SUM(total_views) AS total_views,
                SUM(unique_users) AS unique_users
            FROM viewership_summary
            GROUP BY show_id
            ORDER BY total_minutes DESC
            LIMIT 10
        """,

        "Daily Engagement": """
            SELECT
                view_date,
                SUM(total_views) AS total_views,
                SUM(total_watch_time) AS total_minutes,
                ROUND(SUM(total_watch_time)/60.0, 2) AS total_hours
            FROM viewership_summary
            GROUP BY view_date
            ORDER BY view_date DESC
        """
    }

    for title, query in queries.items():
        print("\n" + "=" * 80)
        print(title)
        print("=" * 80)
        df = pd.read_sql_query(query, conn)
        print(df.to_string(index=False))


# ================= MAIN =================
def main():
    print("\n" + "=" * 80)
    print("OTT ANALYTICS – SQL ANALYSIS (SQLite)")
    print("=" * 80)

    conn = create_database()
    run_queries(conn)
    conn.close()

    print("\n[OK] SQL analytics completed successfully")


if __name__ == "__main__":
    main()
