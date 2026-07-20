# GitHub Events Analytics Pipeline

An end-to-end data engineering pipeline that ingests live GitHub public events, transforms them through a 3-layer warehouse architecture, monitors data quality, and visualizes analytics in a real-time dashboard.

![Pipeline Architecture](https://img.shields.io/badge/Python-3.11-blue) ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue) ![Airflow](https://img.shields.io/badge/Airflow-2.9.1-green) ![Docker](https://img.shields.io/badge/Docker-29.6-blue) ![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red)

---

## Architecture

```
GitHub Events API
       ↓
fetch_events.py (REST ingestion)
       ↓
Raw JSON files (data/raw/)
       ↓
load_to_db.py → PostgreSQL (raw layer)
       ↓
transform.py → Staging + Mart tables
       ↓
Data Quality Checks (row count, null rate, freshness)
       ↓
Streamlit Dashboard (real-time analytics)
       ↑
Apache Airflow DAG (hourly orchestration)
```

---

## What It Does

- **Ingests** 100+ live events per run from the GitHub public events API with authentication and rate-limit handling
- **Stores** raw JSON snapshots with timestamps for full audit trail
- **Transforms** through a 3-layer warehouse: raw → staging (cleaned, enriched) → mart (aggregated analytics)
- **Monitors** data quality on every run — pipeline fails loudly if row count drops below threshold or null rate exceeds 5%
- **Visualizes** event distributions, top actors, and recent activity in a Streamlit dashboard
- **Orchestrates** the full pipeline as an Airflow DAG on an hourly schedule with retry logic

---

## Tech Stack

| Layer | Tool | Why |
|---|---|---|
| Ingestion | Python + requests | REST API with auth headers and error handling |
| Storage | PostgreSQL 15 | Production-grade relational DB with ACID guarantees |
| Transformation | Python + psycopg2 | 3-layer warehouse pattern (raw → staging → mart) |
| Orchestration | Apache Airflow 2.9.1 | Industry standard for pipeline scheduling and monitoring |
| Quality | Custom checks | Row count, null rate, data freshness validation |
| Dashboard | Streamlit + Plotly | Interactive analytics with real-time data |
| Containerization | Docker + docker-compose | Reproducible environment, one-command startup |

---

## Data Model

### Raw Layer — `github_events`
Exact copy of API response, minimal transformation
```sql
event_id    VARCHAR(50) PRIMARY KEY
event_type  VARCHAR(50)
actor       VARCHAR(100)
repo_name   VARCHAR(200)
created_at  TIMESTAMP
ingested_at TIMESTAMP
```

### Staging Layer — `stg_github_events`
Cleaned, enriched, deduplicated
```sql
event_id    VARCHAR(50) PRIMARY KEY
event_type  VARCHAR(50)
actor       VARCHAR(100)
repo_name   VARCHAR(200)
repo_owner  VARCHAR(100)   -- extracted from repo_name
created_at  TIMESTAMP
ingested_at TIMESTAMP
processed_at TIMESTAMP
```

### Mart Layer — `mart_event_summary`
Aggregated analytics, query-optimized
```sql
summary_date  DATE
event_type    VARCHAR(50)
total_events  INT
unique_actors INT
unique_repos  INT
PRIMARY KEY (summary_date, event_type)
```

---

## Data Quality Checks

Every pipeline run validates:

| Check | Threshold | Action on failure |
|---|---|---|
| Row count | > 50 events | Pipeline stops, error logged |
| Null actor rate | < 5% | Pipeline stops, error logged |
| Data freshness | Events within last 24 hrs | Warning logged |

---

## Airflow DAG

```python
fetch_and_save_events >> load_to_postgres >> run_transformations >> data_quality_checks
```

- Schedule: `@hourly`
- Retries: 1 (with 2-minute delay)
- Tags: `data-engineering`, `github`, `pipeline`
- Metadata DB: PostgreSQL (production-grade, not SQLite)

---

## Dashboard

Real-time Streamlit dashboard showing:
- Total events, unique actors, unique repos, event types (metric cards)
- Event type distribution (bar chart)
- Top 10 most active GitHub users (horizontal bar chart)
- Recent events table with filtering

---

## Setup

### Prerequisites
- Python 3.11
- Docker Desktop
- GitHub Personal Access Token (public_repo scope)

### Run locally

```bash
# Clone the repo
git clone https://github.com/ManyaEleti/github-events-pipeline.git
cd github-events-pipeline

# Create conda environment
conda create -n github-pipeline python=3.11 -y
conda activate github-pipeline

# Install dependencies
pip install requests psycopg2-binary pandas python-dotenv streamlit plotly apache-airflow==2.9.1

# Set up environment variables
cp .env.example .env
# Add your GITHUB_TOKEN to .env

# Start PostgreSQL
docker-compose up -d

# Run the pipeline
python ingestion/fetch_events.py
python ingestion/save_events.py
python ingestion/load_to_db.py
python ingestion/transform.py

# Launch dashboard
cd dashboard && streamlit run app.py
```

### Start Airflow

```bash
export AIRFLOW_HOME=~/github-events-pipeline/airflow
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://pipeline_user:pipeline_pass@localhost:5432/airflow_metadata
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow standalone
```

---

## Results

| Metric | Value |
|---|---|
| Events per run | 100 |
| Unique actors per run | ~94 |
| Pipeline run time | < 30 seconds |
| Data quality pass rate | 100% |
| Event types captured | PushEvent, CreateEvent, PullRequestEvent, IssuesEvent |

---

## What I'd Add Next

- **dbt** for SQL-based transformations with version control and testing
- **Kafka** for real-time streaming instead of batch ingestion
- **GCP BigQuery** as the warehouse for true cloud-scale analytics
- **Great Expectations** for more comprehensive data quality suite
- **Grafana** for production monitoring dashboards
- **CI/CD** with GitHub Actions for automated testing on every push

---

## Project Structure

```
github-events-pipeline/
├── ingestion/
│   ├── fetch_events.py      # GitHub API ingestion
│   ├── save_events.py       # Raw JSON storage
│   ├── load_to_db.py        # PostgreSQL loader
│   └── transform.py         # 3-layer transformation
├── dashboard/
│   └── app.py               # Streamlit dashboard
├── airflow/
│   └── dags/
│       └── github_pipeline_dag.py  # Airflow orchestration
├── data/
│   └── raw/                 # Raw JSON snapshots
├── docker-compose.yml       # PostgreSQL container
├── .env.example             # Environment template
└── README.md
```

---

## Author

**Manya Eleti** — MS Data Science, UMBC

[LinkedIn](https://linkedin.com/in/manyaeleti) · [GitHub](https://github.com/ManyaEleti)
