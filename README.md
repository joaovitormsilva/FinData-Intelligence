# FinData-Intelligence

An end-to-end Data Engineering platform for automated cryptocurrency market data ingestion, transformation, and analytics. Built on a **Medallion Architecture** (Bronze → Silver → Gold) fully containerized with Docker.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Orchestration                            │
│                      Apache Airflow 3.0.6                       │
│                                                                 │
│  DAG: bronze_ingestion          DAG: dbt_transform              │
│  ┌─────────────────┐            ┌──────────────────┐            │
│  │ extract         │            │ dbt seed         │            │
│  │ transform       │  triggers  │ dbt staging.*    │            │
│  │ load_stg        │ ─────────► │ dbt silver.*     │            │
│  │ load_bronze     │            │ dbt test         │            │
│  │ read_table      │            └──────────────────┘            │
│  └─────────────────┘                                            │
└─────────────────────────────────────────────────────────────────┘
         │ API                              │ CSV seed
         ▼                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                     PostgreSQL 17                               │
│                                                                 │
│  BRONZE schema                  dbt_stg schema                  │
│  ┌────────────────────────┐     ┌──────────────────────────┐    │
│  │ precos_historicos_     │     │ stg_precos_historicos    │    │
│  │ ativos_crypto          │     │ (view)                   │    │
│  │ (partitioned by date)  │     │                          │    │
│  │                        │     │ btc_eur                  │    │
│  │ stg_precos_historicos_ │     │ (view)                   │    │
│  │ ativos_crypto          │     └──────────────────────────┘    │
│  └────────────────────────┘                                     │
│                                                                 │
│  dbt_silver schema              dbt_gold schema                 │
│  ┌────────────────────────┐     ┌──────────────────────────┐    │
│  │ s_precos_historicos    │     │ (in progress)            │    │
│  │ s_btc_eur              │     └──────────────────────────┘    │
│  └────────────────────────┘                                     │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────┐
│   Metabase      │  ◄── coming soon
│   (BI layer)    │
└─────────────────┘
```

### Medallion Architecture

| Layer | Technology | Responsibility |
|---|---|---|
| **Bronze** | PostgreSQL + Stored Procedure | Raw data from API, partitioned by date, retention policy |
| **Staging** | dbt (view) | Null filtering, type casting, column standardization |
| **Silver** | dbt (table) | Business transformations — daily variance, max range |
| **Gold** | dbt (table) | Aggregated metrics for BI consumption (in progress) |

### Key Technical Decisions

**Partitioning by date on Bronze** — The `precos_historicos_ativos_crypto` table is range-partitioned by `date` via a PostgreSQL stored procedure (`sp_load_historico_crypto`). This supports efficient time-range queries as data volume grows and enables partition-level retention management.

**Staging as views** — The staging layer is materialized as views (`+materialized: view`) to avoid unnecessary storage of intermediate data. Views are computed on demand by the silver models.

**Staging table pattern for Bronze loading** — Raw data is first written to `stg_precos_historicos_ativos_crypto` (a plain table), then the stored procedure promotes it to the partitioned bronze table and truncates staging. This isolates the write concern from the partitioning logic.

**Two ingestion sources converging at Silver** — API data (AAVE/GBP via Alpha Vantage) and historical CSV data (BTC/EUR) follow separate paths through Bronze and Staging respectively, converging in the Silver layer for unified analytics.

**DAG dependency via TriggerDagRunOperator** — `bronze_ingestion` triggers `dbt_transform` upon completion using `TriggerDagRunOperator` with `wait_for_completion=True`, ensuring transformations only run on fresh data.

---

## Stack

| Component | Version |
|---|---|
| Apache Airflow | 3.0.6 |
| dbt-core | 1.11.6 |
| dbt-postgres | 1.11.6 |
| PostgreSQL | 17 |
| Python | 3.x |
| Docker / Docker Compose | latest |
| Metabase | coming soon |

---

## Project Structure

```
FinData-Intelligence/
│
├── airflow/
│   ├── dags/
│   │   ├── bronze_ingestion.py     # Ingestion DAG: API → Bronze
│   │   └── dbt_transform.py        # Transformation DAG: Staging → Silver → Test
│   ├── logs/                       # Airflow task logs (gitignored)
│   ├── plugins/                    # Airflow plugins (unused)
│   └── config/                     # airflow.cfg (gitignored)
│
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── precos_historicos.sql   # View over bronze API data
│   │   │   ├── btc_eur.sql             # View over BTC/EUR seed data
│   │   │   ├── schema.yml
│   │   │   └── __sources.yml
│   │   ├── silver/
│   │   │   ├── s_precos_historicos.sql # AAVE/GBP with daily variance
│   │   │   ├── s_btc_eur.sql           # BTC/EUR with daily variance
│   │   │   └── schema.yml              # Tests: not_null, is_positive, accepted_values
│   │   └── gold/                       # In progress
│   ├── seeds/
│   │   └── btc/
│   │       └── currency_daily_btc_eur.csv
│   ├── macros/
│   │   └── test_is_positive.sql        # Custom generic test
│   ├── profiles.yml
│   └── dbt_project.yml
│
├── src/
│   └── ingestion/
│       ├── crypto_api.py       # Alpha Vantage API client
│       ├── tb_create.py        # API response → list of dicts
│       ├── connect_db.py       # SQLAlchemy engine factory
│       ├── write_stg_table.py  # Insert to staging with rowcount warning
│       ├── call_procedure.py   # Calls sp_load_historico_crypto
│       ├── read_db.py          # Generic SELECT helper
│       └── ingestion.py        # Local dev runner (not for production)
│
├── scripts/
│   └── init.sql                # Schema creation + stored procedure
│
├── airflow.Dockerfile
├── test_etl.Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .env                        # Not committed — see setup below
```

---

## Getting Started

### Prerequisites

- Docker and Docker Compose installed
- Minimum 4GB RAM and 2 CPUs available for Docker
- An [Alpha Vantage API key](https://www.alphavantage.co/support/#api-key) (free tier available)

### 1. Clone the repository

```bash
git clone https://github.com/joaovitormsilva/FinData-Intelligence.git
cd FinData-Intelligence
```

### 2. Create the `.env` file

Create a `.env` file at the project root with the following variables:

```env
# PostgreSQL
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
POSTGRES_DB=fin_data_db
POSTGRES_HOST=data_center_db

# Airflow
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=your_airflow_password
FERNET_KEY=your_fernet_key
SECRET_KEY=your_secret_key

# Alpha Vantage
ALPHA_VANTAGE_API_KEY=your_api_key
```

> **Security note:** The Alpha Vantage API key should be rotated every 60 days. Never commit the `.env` file — it is already in `.gitignore`.

To generate a valid Fernet key:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 3. Start the services

```bash
docker compose up --build
```

On first run, `airflow-init` will run migrations, create the admin user, and set up the required directories. Wait for it to complete before accessing the UI.

### 4. Access the services

| Service | URL | Default credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | Defined in `.env` |
| PostgreSQL | localhost:5432 | Defined in `.env` |

### 5. Run the pipeline

In the Airflow UI:

1. Enable and trigger `bronze_ingestion` manually
2. It will automatically trigger `dbt_transform` upon completion
3. Monitor both DAGs in the Graph view

---

## Data Pipeline Flow

```
Alpha Vantage API
      │
      ▼
extract()               # Fetches DIGITAL_CURRENCY_DAILY for AAVE/GBP
      │
transform()             # Parses API response into list of dicts
      │
load_stg()              # Inserts into bronze.stg_precos_historicos_ativos_crypto
      │                   ON CONFLICT DO NOTHING (idempotent)
load_bronze()           # Calls sp_load_historico_crypto(30)
      │                   Creates date partitions, loads from staging, truncates staging
      │                   Drops partitions older than 30 days
read_table()            # Validation read — logs sample from bronze
      │
trigger dbt_transform
      │
      ├── dbt seed       # Loads seeds/btc/currency_daily_btc_eur.csv → dbt_stg
      ├── dbt staging.*  # Creates views: precos_historicos, btc_eur
      ├── dbt silver.*   # Creates tables: s_precos_historicos, s_btc_eur
      └── dbt test       # Runs: not_null, is_positive, accepted_values, unique
```

---

## dbt Data Quality Tests

The following generic tests are applied to all Silver models:

| Test | Columns | Models |
|---|---|---|
| `not_null` | All columns | `s_precos_historicos`, `s_btc_eur` |
| `unique` | `date` | `s_precos_historicos`, `s_btc_eur` |
| `is_positive` | `open`, `high`, `low`, `close`, `volume` | Both |
| `accepted_values` | `dgt_crrnc_cd` | `AAVE` or `BTC` per model |
| `accepted_values` | `mrkt_cd` | `GBP` or `EUR` per model |

`is_positive` is a custom generic test defined in `dbt/macros/test_is_positive.sql`.

---

## Environment Variables Reference

| Variable | Description | Used by |
|---|---|---|
| `POSTGRES_USER` | Database user | Airflow, dbt, PostgreSQL |
| `POSTGRES_PASSWORD` | Database password | Airflow, dbt, PostgreSQL |
| `POSTGRES_DB` | Database name | Airflow, dbt, PostgreSQL |
| `POSTGRES_HOST` | Database host (Docker service name) | Airflow, dbt |
| `FERNET_KEY` | Airflow encryption key for connections | Airflow |
| `SECRET_KEY` | Airflow webserver and API secret | Airflow |
| `ALPHA_VANTAGE_API_KEY` | External API key — rotate every 60 days | `crypto_api.py` |
| `AIRFLOW_UID` | Host user UID for volume permissions | Airflow init |

---

## Roadmap

- [ ] Gold layer models for consolidated analytics
- [ ] Metabase integration for BI dashboards
- [ ] Additional crypto assets ingestion
- [ ] `on_failure_callback` alerts on DAG failure
- [ ] Airflow Datasets for event-driven DAG triggering
- [ ] dbt incremental models for Silver layer