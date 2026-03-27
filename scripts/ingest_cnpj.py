import requests
import time
import json
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime, timezone

# Configurações
CNPJS = [
    "00000000000191",  # Banco do Brasil
    "60701190000104",  # Itaú
    "33000167000101",  # Petrobras
    "00360305000104",  # Caixa Econômica
    "59285411000113",  # Magazine Luiza
]

PROJECT_ID = "crm-estudo-bq"
BUCKET_NAME = "olist_raw_data_ptf"
DATASET = "bronze"
TABLE = "raw_cnpj"

bq_client = bigquery.Client(project=PROJECT_ID)
gcs_client = storage.Client(project=PROJECT_ID)


def fetch_cnpj(cnpj: str) -> dict:
    url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}"
    response = requests.get(url, timeout=10)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erro ao buscar CNPJ {cnpj}: {response.status_code}")
        return None


def save_to_gcs(records: list) -> str:
    date_partition = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    filename = f"cnpj/{date_partition}/raw_cnpj.jsonl"

    bucket = gcs_client.bucket(BUCKET_NAME)
    blob = bucket.blob(filename)

    jsonl_content = "\n".join(json.dumps(record) for record in records)
    blob.upload_from_string(jsonl_content, content_type="application/json")

    print(f"Arquivo salvo em gs://{BUCKET_NAME}/{filename}")
    return f"gs://{BUCKET_NAME}/{filename}"


def load_to_bigquery(gcs_uri: str) -> None:
    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = bq_client.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=job_config,
    )

    load_job.result()
    print(f"Dados carregados em {table_ref}")


def main():
    records = []

    for cnpj in CNPJS:
        print(f"Buscando CNPJ: {cnpj}")
        data = fetch_cnpj(cnpj)

        if data:
            data["ingested_at"] = datetime.now(timezone.utc).isoformat()
            records.append(data)

        time.sleep(1)

    if records:
        gcs_uri = save_to_gcs(records)
        load_to_bigquery(gcs_uri)


if __name__ == "__main__":
    main()