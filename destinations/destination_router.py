from destinations.mysql_writer import push_to_mysql
from destinations.postgres_writer import push_postgres
from destinations.bigquery_writer import push_bigquery
from destinations.snowflake_writer import push_snowflake
from destinations.clickhouse_writer import push_clickhouse
from destinations.s3_writer import push_s3
from security.secure_db import decrypt_payload

import sqlite3

DB = "identity.db"

def resolve_destination_format(dest_cfg, source):

    dest_type = dest_cfg.get("type")

    # only needed for supported destinations
    if dest_type not in ["s3", "bigquery"]:
        return dest_cfg

    try:
        con = sqlite3.connect(DB)
        cur = con.cursor()

        cur.execute("""
            SELECT format
            FROM destination_configs
            WHERE source=?
            AND dest_type=?
            AND is_active=1
            ORDER BY id DESC
            LIMIT 1
        """, (source, dest_type))

        row = cur.fetchone()
        con.close()

        if row and row[0]:
            dest_cfg["format"] = row[0].lower()
            print("[ROUTER] FORMAT RESOLVED FROM DB:", row[0])

    except Exception as e:
        print("[ROUTER FORMAT ERROR]", e)

    return dest_cfg

def push_to_destination(dest_cfg, source, rows):

    if not rows:
        return 0

    # FORCE DECRYPT DESTINATION CONFIG
    dest_cfg = decrypt_payload(dict(dest_cfg))

    # CENTRAL FORMAT RESOLUTION
    dest_cfg = resolve_destination_format(dest_cfg, source)

    dest_type = dest_cfg["type"]

    # ---------------- FORMAT ISOLATION ----------------
    if dest_type in ["bigquery", "s3"]:
        dest_cfg["format"] = (
            dest_cfg.get("format") or "parquet"
        ).lower()
    else:
        dest_cfg.pop("format", None)

    if dest_type == "mysql":

        return push_to_mysql(dest_cfg, source, rows)

    elif dest_type == "postgres":

        return push_postgres(dest_cfg, source, rows)

    elif dest_type == "bigquery":

        return push_bigquery(dest_cfg, source, rows)

    elif dest_type == "snowflake":

        return push_snowflake(dest_cfg, source, rows)

    elif dest_type == "clickhouse":
        return push_clickhouse(dest_cfg, source, rows)
    
    elif dest_type == "s3":
        return push_s3(dest_cfg, source, rows)

    else:

        raise Exception(f"Unsupported destination: {dest_type}")