print("### BIGQUERY PARQUET WRITER LOADED ###")

import json
import tempfile
import os
import pandas as pd

from google.cloud import bigquery
from google.oauth2 import service_account


def push_bigquery(dest, source, rows):

    if not rows:
        return 0

    print("[BIGQUERY] Using PARQUET loader")

    # Credentials
    creds_dict = json.loads(dest["password"])

    credentials = service_account.Credentials.from_service_account_info(
        creds_dict
    )

    client = bigquery.Client(
        credentials=credentials,
        project=dest["host"]
    )

    project_id = dest["host"]
    dataset_id = dest["database_name"]
    table_id = f"{project_id}.{dataset_id}.{source}_data"

    # Dataset (create if not exists)
    dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")

    try:
        client.create_dataset(dataset_ref)
        print("[BIGQUERY] Dataset created")
    except Exception:
        pass

    # DataFrame Conversion
    df = pd.DataFrame(rows)

    # Convert ONLY connector fields to STRING
    for col in df.columns:
        df[col] = df[col].astype(str)

    df["fetched_at"] = pd.Timestamp.utcnow()

    # Write TEMP PARQUET
    tmp = tempfile.NamedTemporaryFile(
        delete=False,
        suffix=".parquet"
    )

    parquet_path = tmp.name
    tmp.close()

    df.to_parquet(
        parquet_path,
        engine="pyarrow",
        index=False
    )

    print(
        "[BIGQUERY] Parquet size:",
        os.path.getsize(parquet_path),
        "bytes"
    )

    # Load into BigQuery
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_APPEND",
        autodetect=True
    )

    with open(parquet_path, "rb") as f:

        job = client.load_table_from_file(
            f,
            table_id,
            job_config=job_config
        )

    job.result()

    os.unlink(parquet_path)

    print(f"[BIGQUERY] Loaded {len(rows)} rows via PARQUET")

    return len(rows)