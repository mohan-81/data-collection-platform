print("### S3 PARQUET WRITER LOADED ###")

import json
import tempfile
import os
import time
import pandas as pd
import boto3

from datetime import datetime


def push_s3(dest, source, rows):

    if not rows:
        return 0

    print("[S3] Preparing parquet upload")

    # AWS Credentials
    aws_access_key = dest["username"]
    aws_secret_key = dest["password"]
    bucket_name = dest["host"]
    region = dest.get("port") or "us-east-1"

    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region
    )

    # DataFrame → Parquet
    df = pd.DataFrame(rows)

    for col in df.columns:
        df[col] = df[col].astype(str)

    df["fetched_at"] = pd.Timestamp.utcnow()

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

    # Partitioned Path
    now = datetime.utcnow()

    key = (
        f"{source}/"
        f"year={now.year}/"
        f"month={now.month:02}/"
        f"day={now.day:02}/"
        f"{source}_{int(time.time())}.parquet"
    )

    print("[S3] Uploading:", key)

    # Upload
    s3.upload_file(
        parquet_path,
        bucket_name,
        key
    )

    os.unlink(parquet_path)

    print(f"[S3] Uploaded {len(rows)} rows")

    return len(rows)