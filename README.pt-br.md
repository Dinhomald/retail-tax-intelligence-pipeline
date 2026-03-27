# retail-tax-intelligence-pipeline

Pipeline de dados end-to-end para análise de empresas brasileiras com foco tributário, construído com stack GCP nativa.

## Arquitetura
```
BrasilAPI → Python → GCS → BigQuery Bronze → dbt → Silver → Gold
```

## Stack

- **Ingestão:** Python + BrasilAPI (CNPJ)
- **Storage:** Google Cloud Storage (JSONL particionado por data)
- **Data Warehouse:** BigQuery (arquitetura Medallion)
- **Transformação:** dbt Core
- **Orquestração (produção):** Cloud Run Jobs + Cloud Scheduler

## Camadas

| Camada | Dataset | Descrição |
|---|---|---|
| Bronze | `bronze` | Dados brutos ingeridos via API |
| Silver | `silver` | Dados tratados e padronizados |
| Gold | `gold` | Agregações e visão analítica |

## Como rodar

### Pré-requisitos
- Python 3.10+
- gcloud CLI autenticado
- dbt-bigquery instalado

### Ingestão
```bash
python ingest_cnpj.py
```

### Transformação
```bash
dbt run
```