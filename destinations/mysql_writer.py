import mysql.connector
from datetime import datetime


def push_to_mysql(dest, source, rows):

    if not rows:
        return


    conn = mysql.connector.connect(
        host=dest["host"],
        port=int(dest["port"]),
        user=dest["username"],
        password=dest["password"],
        database=dest["database_name"]
    )

    cur = conn.cursor()


    # ---------- Build Table Name ----------
    table = f"{source}_data"


    # ---------- Create Table ----------
    columns = rows[0].keys()

    col_defs = []

    for c in columns:
        col_defs.append(f"`{c}` TEXT")

    col_sql = ", ".join(col_defs)


    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            {col_sql},
            fetched_at DATETIME
        )
    """

    cur.execute(create_sql)


    # ---------- Insert Rows ----------
    cols = list(columns)
    placeholders = ", ".join(["%s"] * len(cols))
    col_names = ", ".join([f"`{c}`" for c in cols])

    insert_sql = f"""
        INSERT INTO {table}
        ({col_names}, fetched_at)
        VALUES ({placeholders}, %s)
    """


    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


    count = 0

    for r in rows:

        try:

            values = [str(r.get(c)) for c in cols]
            values.append(now)

            cur.execute(insert_sql, values)

            count += 1

        except Exception as e:

            print("[MYSQL] Insert error:", e)


    conn.commit()
    conn.close()

    print(f"[DEST] Pushed {count} rows to MySQL ({table})")
    
    return count