import snowflake.connector
import json


def push_snowflake(dest, source, rows):

    conn = snowflake.connector.connect(
        user=dest["username"],
        password=dest["password"],
        account=dest["host"],
        warehouse="COMPUTE_WH",
        database=dest["database_name"],
        schema="PUBLIC"
    )

    cur = conn.cursor()

    table = f"{source}_data"


    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id INTEGER AUTOINCREMENT,
            payload VARIANT
        )
    """)


    count = 0

    for r in rows:

        cur.execute(
            f"INSERT INTO {table} (payload) SELECT PARSE_JSON(%s)",
            (json.dumps(r),)
        )

        count += 1


    conn.commit()
    cur.close()
    conn.close()

    print(f"[DEST] Pushed {count} rows to Snowflake")

    return count