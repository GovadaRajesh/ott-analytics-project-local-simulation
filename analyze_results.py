"""
Analyze Processed ETL Results (LOCAL)
Run: python scripts/analyze_results.py
"""

import pandas as pd
import json
import os
from glob import glob
from datetime import datetime

# ================= PATHS =================
SUMMARY_DIR = r"C:\Users\abbay\data\processed\summary"
REPORT_FILE = r"C:\Users\abbay\data\processed\analysis_report.txt"


def load_json_data():
    files = glob(os.path.join(SUMMARY_DIR, "viewership_summary-*.json"))

    if not files:
        raise FileNotFoundError("No summary JSON files found")

    records = []
    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))

    if not records:
        raise ValueError("Summary JSON files are empty")

    df = pd.DataFrame(records)
    print(f"[OK] Loaded records: {len(df)}")
    print(f"[OK] Columns: {list(df.columns)}")

    return df


def analyze_show_performance(df):
    print("\n" + "=" * 80)
    print("SHOW PERFORMANCE ANALYSIS")
    print("=" * 80)

    top = df.nlargest(10, "total_watch_time")

    for _, r in top.iterrows():
        print(
            f"{r['show_id']:6s} | "
            f"{r['total_watch_time']/60:8.1f} hrs | "
            f"{int(r['total_views']):6,d} views | "
            f"{int(r['unique_users']):5,d} users | "
            f"Avg {r['avg_duration']:5.1f} min"
        )


def export_report(df):
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write("OTT ANALYTICS REPORT\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now()}\n\n")
        f.write(f"Total Watch Time: {df['total_watch_time'].sum()/60:.1f} hrs\n")
        f.write(f"Active Shows: {df['show_id'].nunique()}\n")

    print(f"[OK] Report generated at: {REPORT_FILE}")


def main():
    print("\nOTT ANALYTICS – DATA ANALYSIS")

    df = load_json_data()
    analyze_show_performance(df)
    export_report(df)

    print("[OK] Analysis completed successfully")


if __name__ == "__main__":
    main()
