print("### BIGQUERY WRITER FILE LOADED ###")

import json
import tempfile
import os
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timezone


def push_bigquery(dest, source, rows):
    print("### USING FILE-BASED BIGQUERY LOADER ###")

    if not rows:
        return 0


    print("### USING FILE-BASED BIGQUERY LOADER ###")


    # -------------------------------
    # Credentials
    # -------------------------------

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


    # -------------------------------
    # Dataset
    # -------------------------------

    dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")

    try:
        client.create_dataset(dataset_ref)
        print("[BIGQUERY] Dataset created")

    except Exception:
        pass


    # -------------------------------
    # Schema
    # -------------------------------

    schema = []

    for col in rows[0].keys():
        schema.append(bigquery.SchemaField(col, "STRING"))

    schema.append(bigquery.SchemaField("fetched_at", "TIMESTAMP"))


    table = bigquery.Table(table_id, schema=schema)

    try:
        client.create_table(table)
        print(f"[BIGQUERY] Table created: {table_id}")

    except Exception:
        pass


    # -------------------------------
    # Write Temp File (JSONL)
    # -------------------------------

    tmp = tempfile.NamedTemporaryFile(
        mode="w",
        delete=False,
        suffix=".json"
    )


    for r in rows:

        row = {}

        for k, v in r.items():
            row[k] = str(v)

        row["fetched_at"] = datetime.now(timezone.utc).isoformat()

        tmp.write(json.dumps(row) + "\n")


    tmp.close()


    # -------------------------------
    # Load Job (NO STREAMING)
    # -------------------------------

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_APPEND"
    )


    with open(tmp.name, "rb") as f:

        job = client.load_table_from_file(
            f,
            table_id,
            job_config=job_config
        )


    job.result()


    os.unlink(tmp.name)


    print(f"[BIGQUERY] Loaded {len(rows)} rows")

    return len(rows)