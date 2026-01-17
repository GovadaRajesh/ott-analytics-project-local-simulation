"""
Simulated Airflow DAG for OTT Analytics Pipeline (LOCAL)

Run:
python dags/ott_pipeline_dag.py
"""

from datetime import datetime
import os
import subprocess

# =============================================================================
# CONFIG
# =============================================================================

USER_HOME = r"C:\Users\abbay"

# Possible raw file locations (auto-detect)
RAW_FILE_CANDIDATES = [
    os.path.join(USER_HOME, "view_log_2025-12-20.csv"),
    os.path.join(USER_HOME, "data", "raw", "view_log_2025-12-20.csv"),
]

PROCESSED_DIR = os.path.join(USER_HOME, "data", "processed")

ETL_SCRIPT = r"scripts\dataflow_local_etl.py"
ANALYTICS_SCRIPT = r"scripts\sql_analytics.py"
REPORT_SCRIPT = r"scripts\analyze_results.py"

SUMMARY_FILE = os.path.join(PROCESSED_DIR, "viewership_summary.json")
REPORT_FILE = os.path.join(PROCESSED_DIR, "analysis_report.txt")

DAG_CONFIG = {
    "dag_id": "ott_viewership_pipeline",
    "schedule": "0 2 * * *",
    "start_date": "2025-12-01",
    "catchup": False,
}

# =============================================================================
# HELPERS
# =============================================================================

def find_raw_file():
    """Return first existing raw file path"""
    for path in RAW_FILE_CANDIDATES:
        if os.path.exists(path):
            return path
    return None

# =============================================================================
# SIMULATED AIRFLOW CLASSES
# =============================================================================

class Task:
    def __init__(self, task_id, func, dependencies=None):
        self.task_id = task_id
        self.func = func
        self.dependencies = dependencies or []
        self.status = "pending"

    def execute(self):
        print("\n" + "=" * 80)
        print(f"EXECUTING TASK: {self.task_id}")
        print("=" * 80)

        try:
            self.func()
            self.status = "success"
            print(f"✓ Task {self.task_id} completed successfully")
            return True
        except Exception as e:
            self.status = "failed"
            print(f" Task {self.task_id} failed")
            print(f"Error: {e}")
            return False


class SimulatedDAG:
    def __init__(self, dag_id):
        self.dag_id = dag_id
        self.tasks = []

    def add_task(self, task):
        self.tasks.append(task)

    def run(self):
        print("\n" + "=" * 80)
        print(f"STARTING DAG: {self.dag_id}")
        print("=" * 80)

        for task in self.tasks:
            for dep in task.dependencies:
                if dep.status != "success":
                    print(f" Skipping {task.task_id} (dependency failed)")
                    task.status = "skipped"
                    return False

            if not task.execute():
                print("\n DAG FAILED")
                return False

        print("\n✓ DAG COMPLETED SUCCESSFULLY")
        return True


# =============================================================================
# TASK FUNCTIONS
# =============================================================================

def check_raw_data():
    print("Checking raw input data...")

    raw_file = find_raw_file()

    if not raw_file:
        raise FileNotFoundError(
            "Raw file not found in any expected location:\n"
            + "\n".join(RAW_FILE_CANDIDATES)
        )

    size = os.path.getsize(raw_file)
    print(f"✓ Found raw file: {raw_file}")
    print(f"  Size: {size:,} bytes")


def run_etl_pipeline():
    print("Running ETL pipeline (Apache Beam DirectRunner)...")

    result = subprocess.run(
        ["python", ETL_SCRIPT],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(result.stderr)
        raise Exception("ETL pipeline failed")

    print("✓ ETL pipeline completed")


def validate_output():
    print("Validating ETL output...")

    if not os.path.exists(SUMMARY_FILE):
        raise FileNotFoundError("Missing ETL output: viewership_summary.json")

    size = os.path.getsize(SUMMARY_FILE)
    print(f"✓ Found summary file ({size:,} bytes)")


def run_sql_analytics():
    print("Running SQL analytics (SQLite simulation)...")

    result = subprocess.run(
        ["python", ANALYTICS_SCRIPT],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(result.stderr)
        raise Exception("SQL analytics failed")

    print("✓ SQL analytics completed")


def generate_report():
    print("Generating analytics report...")

    result = subprocess.run(
        ["python", REPORT_SCRIPT],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(result.stderr)
        raise Exception("Report generation failed")

    if os.path.exists(REPORT_FILE):
        print(f"✓ Report generated: {REPORT_FILE}")


def send_notification():
    print("Sending notification (simulated)...")
    print("✓ Email sent to data-team@company.com")


# =============================================================================
# DAG BUILDER
# =============================================================================

def build_dag():
    dag = SimulatedDAG(DAG_CONFIG["dag_id"])

    t1 = Task("check_raw_data", check_raw_data)
    t2 = Task("run_etl_pipeline", run_etl_pipeline, [t1])
    t3 = Task("validate_output", validate_output, [t2])
    t4 = Task("run_sql_analytics", run_sql_analytics, [t3])
    t5 = Task("generate_report", generate_report, [t4])
    t6 = Task("send_notification", send_notification, [t5])

    for t in [t1, t2, t3, t4, t5, t6]:
        dag.add_task(t)

    return dag


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    print("""
╔════════════════════════════════════════════════════════════════════════╗
║              OTT ANALYTICS PIPELINE – SIMULATED AIRFLOW DAG             ║
╚════════════════════════════════════════════════════════════════════════╝
""")

    print("DAG Configuration:")
    for k, v in DAG_CONFIG.items():
        print(f"  {k}: {v}")

    input("\nPress ENTER to start DAG execution...")

    dag = build_dag()
    success = dag.run()

    exit(0 if success else 1)
