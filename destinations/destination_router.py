from destinations.mysql_writer import push_to_mysql
from destinations.postgres_writer import push_postgres
from destinations.bigquery_writer import push_bigquery
from destinations.snowflake_writer import push_snowflake
from destinations.clickhouse_writer import push_clickhouse
from destinations.s3_writer import push_s3
from security.secure_db import decrypt_payload

import sqlite3

DB = "identity.db"

def push_to_destination(dest_cfg, source, rows):

    if not rows:
        return 0

    # FORCE DECRYPT DESTINATION CONFIG
    dest_cfg = decrypt_payload(dict(dest_cfg))

    dest_type = dest_cfg["type"]

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