# OTT Analytics Pipeline – End-to-End Local Simulation (Production Style)

##  Overview
This project is a **complete end-to-end Data Engineering pipeline** that simulates a
**real-world OTT analytics system** on a local machine. It is designed to mirror how
production pipelines work on cloud platforms (GCP / AWS), but runs fully locally for
development, testing, and learning purposes.

The pipeline covers **data generation → ETL → analytics → reporting → orchestration**
and follows industry-standard **Bronze–Silver–Gold architecture**.

This project demonstrates **job-ready Data Engineering skills**, not just scripts.

---

##  Architecture

Synthetic OTT Logs (CSV)
│
▼
Data Generation (Python)
│
▼
ETL Processing (Apache Beam – Local)
│
▼
Bronze → Silver → Gold Layers
│
▼
SQL Analytics (SQLite – BigQuery simulated)
│
▼
Final Business Report
│
▼
Orchestration (Airflow DAG / PowerShell)


##  Tech Stack

- **Languages:** Python, SQL
- **ETL Processing:** Apache Beam (DirectRunner – local)
- **Orchestration:** Apache Airflow (simulated locally)
- **Analytics:** SQLite (BigQuery simulation)
- **Architecture:** Bronze–Silver–Gold
- **Automation:** PowerShell
- **OS:** Windows
- **Tools:** Git, VS Code

---

##  Project Structure

ott-analytics-project-local-simulation/
│
├── scripts/
│ ├── generate_sample_logs.py # Generates mock OTT logs
│ ├── dataflow_local_etl.py # ETL pipeline (Apache Beam)
│ ├── sql_analytics.py # SQL analytics (SQLite)
│ └── analyze_results.py # Final report generation
│
├── dags/
│ └── ott_pipeline_dag.py # Simulated Airflow DAG
│
├── data/
│ └── processed/
│ ├── summary/
│ ├── device_summary/
│ ├── top_user/
│ ├── genre_summary/
│ ├── ott_analytics.db
│ └── analysis_report.txt
│
├── run_pipeline.ps1 # One-click pipeline execution
└── README.md


---

##  Pipeline Workflow (Step-by-Step)

### 1️ Data Generation
- Generates synthetic OTT viewing logs (CSV)
- Simulates real user activity data
- Used for testing pipelines without external dependency

 Script:
generate_sample_logs.py

### 2️ ETL Processing (Bronze → Silver → Gold)
- Reads raw CSV logs
- Cleans, transforms, and aggregates data
- Applies schema consistency and deduplication
- Outputs analytics-ready JSON datasets

Script:
dataflow_local_etl.py


Technologies:
- Apache Beam
- Local DirectRunner
- Production-style transformations

---

### 3️ Analytics Layer (SQL)
- Loads processed data into SQLite
- Executes analytical queries
- Simulates BigQuery-style analytics locally

 Script:
sql_analytics.py


---

### 4️ Reporting
- Generates human-readable business report
- Converts analytics into insights
- Final output stored as text summary

 Script:
analyze_results.py


---

### 5️ Orchestration (Airflow Simulation)
- DAG orchestrates the entire pipeline
- Task dependencies
- Sequential execution
- Failure visibility

 Script:
ott_pipeline_dag.py


---

### 6️ One-Click Execution (Windows)
Run entire pipeline with:
```powershell
run_pipeline.ps1


This executes:
Data generation
ETL
Analytics
Reporting

 Analytics Generated

Top watched shows

Device-wise engagement
Genre-based insights
Top users by watch time
Overall platform metrics
Daily activity summary

 Why This Project is Important

This project proves:

End-to-end pipeline thinking
Real-world ETL design
Orchestration knowledge
Analytics readiness
Local → cloud workflow understanding
Production mindset (not tutorial code)

It is designed to be extended to GCP using:

Dataflow
BigQuery
Dataproc
Cloud Composer
