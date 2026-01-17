"""
OTT ANALYTICS – APACHE BEAM ETL PIPELINE
LOCAL EXECUTION (DirectRunner)

Generates:
  - viewership_summary
  - device_summary
  - top_user
  - genre_summary
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime
import json
import os
import csv
import shutil
from io import StringIO
import logging

# ---------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)

# ---------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------
INPUT_FILE = r"C:\Users\abbay\view_log_2025-12-20.csv"
BASE_OUTPUT = r"C:\Users\abbay\data\processed"

SUMMARY_DIR = os.path.join(BASE_OUTPUT, "summary")
DEVICE_DIR = os.path.join(BASE_OUTPUT, "device_summary")
USER_DIR   = os.path.join(BASE_OUTPUT, "top_user")
GENRE_DIR  = os.path.join(BASE_OUTPUT, "genre_summary")

OUTPUT_DIRS = [SUMMARY_DIR, DEVICE_DIR, USER_DIR, GENRE_DIR]

# ---------------------------------------------------------------------
# CLEAN OUTPUT DIRECTORIES (VERY IMPORTANT)
# ---------------------------------------------------------------------
def clean_output_dirs():
    for d in OUTPUT_DIRS:
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

# ---------------------------------------------------------------------
# PARSE CSV
# ---------------------------------------------------------------------
class ParseCSVLine(beam.DoFn):
    def process(self, line):
        try:
            row = next(csv.reader(StringIO(line)))

            # Expected CSV:
            # user_id, show_id, duration_minutes, device_type, view_date
            if len(row) < 5:
                return

            yield {
                "user_id": row[0].strip(),
                "show_id": row[1].strip(),
                "duration_minutes": int(float(row[2])),
                "device_type": row[3].strip(),
                "view_date": row[4].strip(),

                # DEFAULT genre (important fix)
                "genre": "Unknown"
            }

        except Exception as e:
            logging.warning(f"Skipping bad row: {line} | {e}")

# ---------------------------------------------------------------------
# AGGREGATION LOGIC
# ---------------------------------------------------------------------
class AggregateMetrics(beam.CombineFn):

    def create_accumulator(self):
        return {"watch": 0, "views": 0, "users": set()}

    def add_input(self, acc, r):
        acc["watch"] += r["duration_minutes"]
        acc["views"] += 1
        acc["users"].add(r["user_id"])
        return acc

    def merge_accumulators(self, accs):
        merged = self.create_accumulator()
        for a in accs:
            merged["watch"] += a["watch"]
            merged["views"] += a["views"]
            merged["users"].update(a["users"])
        return merged

    def extract_output(self, acc):
        return {
            "total_watch_time": acc["watch"],
            "total_views": acc["views"],
            "unique_users": len(acc["users"])
        }

# ---------------------------------------------------------------------
# PIPELINE
# ---------------------------------------------------------------------
def run_pipeline():

    options = PipelineOptions(
        runner="DirectRunner",
        save_main_session=True
    )

    with beam.Pipeline(options=options) as p:

        parsed = (
            p
            | "Read CSV" >> beam.io.ReadFromText(INPUT_FILE, skip_header_lines=1)
            | "Parse CSV" >> beam.ParDo(ParseCSVLine())
        )

        # ================= VIEWERSHIP SUMMARY =================
        (
            parsed
            | "Key Show Date" >> beam.Map(lambda r: ((r["show_id"], r["view_date"]), r))
            | "Agg Show Date" >> beam.CombinePerKey(AggregateMetrics())
            | "Format Show" >> beam.Map(lambda kv: json.dumps({
                "show_id": kv[0][0],
                "view_date": kv[0][1],
                **kv[1],
                "avg_duration": round(kv[1]["total_watch_time"] / kv[1]["total_views"], 2),
                "processed_ts": datetime.utcnow().isoformat()
            }))
            | "Write Show Summary" >> beam.io.WriteToText(
                os.path.join(SUMMARY_DIR, "viewership_summary"),
                file_name_suffix=".json",
                num_shards=1
            )
        )

        # ================= DEVICE SUMMARY =================
        (
            parsed
            | "Key Device" >> beam.Map(lambda r: (r["device_type"], r))
            | "Agg Device" >> beam.CombinePerKey(AggregateMetrics())
            | "Format Device" >> beam.Map(lambda kv: json.dumps({
                "device_type": kv[0],
                **kv[1]
            }))
            | "Write Device" >> beam.io.WriteToText(
                os.path.join(DEVICE_DIR, "device_summary"),
                file_name_suffix=".json",
                num_shards=1
            )
        )

        # ================= USER SUMMARY =================
        (
            parsed
            | "Key User" >> beam.Map(lambda r: (r["user_id"], r))
            | "Agg User" >> beam.CombinePerKey(AggregateMetrics())
            | "Format User" >> beam.Map(lambda kv: json.dumps({
                "user_id": kv[0],
                **kv[1]
            }))
            | "Write User" >> beam.io.WriteToText(
                os.path.join(USER_DIR, "top_user"),
                file_name_suffix=".json",
                num_shards=1
            )
        )

        # ================= GENRE SUMMARY =================
        (
            parsed
            | "Key Genre" >> beam.Map(lambda r: (r["genre"], r))
            | "Agg Genre" >> beam.CombinePerKey(AggregateMetrics())
            | "Format Genre" >> beam.Map(lambda kv: json.dumps({
                "genre": kv[0],
                **kv[1]
            }))
            | "Write Genre" >> beam.io.WriteToText(
                os.path.join(GENRE_DIR, "genre_summary"),
                file_name_suffix=".json",
                num_shards=1
            )
        )

# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------
if __name__ == "__main__":

    print("\nOTT ANALYTICS – APACHE BEAM ETL (LOCAL)")
    print("Cleaning old output folders...")
    clean_output_dirs()

    print("Running pipeline...")
    run_pipeline()

    print("\nPipeline completed successfully")