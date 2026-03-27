# retail-tax-intelligence-pipeline

End-to-end data pipeline for analyzing Brazilian companies with a tax intelligence focus, built on a native GCP stack.

## Architecture
```
BrasilAPI → Python → GCS → BigQuery Bronze → dbt → Silver → Gold
```

## Stack

- **Ingestion:** Python + BrasilAPI (CNPJ)
- **Storage:** Google Cloud Storage (date-partitioned JSONL)
- **Data Warehouse:** BigQuery (Medallion architecture)
- **Transformation:** dbt Core
- **Orchestration (production):** Cloud Run Jobs + Cloud Scheduler

## Layers

| Layer | Dataset | Description |
|---|---|---|
| Bronze | `bronze` | Raw data ingested from API |
| Silver | `silver` | Cleaned and standardized data |
| Gold | `gold` | Aggregations and analytical views |

## How to run

### Prerequisites
- Python 3.10+
- gcloud CLI authenticated
- dbt-bigquery installed

### Ingestion
```bash
python ingest_cnpj.py
```

### Transformation
```bash
dbt run
```