import os
import tempfile
import time
from datetime import datetime

import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient


def push_azure_datalake(dest, source, rows):
    if not rows:
        return 0

    fmt = (dest.get("format") or "parquet").lower()
    print(f"[ADLS] Upload format: {fmt}")

    account_name = dest["host"]
    file_system = dest.get("port") or "segmento"
    account_key = dest["password"]
    base_path = (dest.get("username") or "").strip("/")

    service = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=account_key
    )

    fs_client = service.get_file_system_client(file_system=file_system)
    try:
        fs_client.create_file_system()
    except Exception:
        pass

    df = pd.DataFrame(rows)
    for col in df.columns:
        df[col] = df[col].astype(str)
    df["fetched_at"] = pd.Timestamp.utcnow()

    if fmt == "parquet":
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
        file_path = tmp.name
        tmp.close()
        df.to_parquet(file_path, engine="pyarrow", index=False)
        extension = "parquet"
    elif fmt == "json":
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        file_path = tmp.name
        tmp.close()
        df.to_json(file_path, orient="records", lines=True)
        extension = "json"
    else:
        raise Exception(f"Unsupported ADLS format: {fmt}")

    now = datetime.utcnow()
    rel_path = (
        f"{source}/"
        f"year={now.year}/"
        f"month={now.month:02}/"
        f"day={now.day:02}/"
        f"{source}_{int(time.time())}.{extension}"
    )
    adls_path = f"{base_path}/{rel_path}" if base_path else rel_path
    print("[ADLS] Uploading:", adls_path)

    file_client = fs_client.get_file_client(adls_path)
    with open(file_path, "rb") as f:
        file_client.upload_data(f, overwrite=True)

    os.unlink(file_path)
    print(f"[ADLS] Uploaded {len(rows)} rows ({fmt})")
    return len(rows)
