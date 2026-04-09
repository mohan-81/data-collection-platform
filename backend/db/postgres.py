import os
import threading
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import psycopg2
from psycopg2.pool import ThreadedConnectionPool

_POOL = None
_LOCK = threading.Lock()


def _ensure_uuid_pk_defaults(conn) -> None:
    """
    Normalize any existing table that has `id` as PRIMARY KEY so inserts can omit id.
    """
    ddl = """
    DO $$
    DECLARE
        rec RECORD;
    BEGIN
        FOR rec IN
            SELECT tc.table_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = current_schema()
              AND kcu.column_name = 'id'
        LOOP
            BEGIN
                EXECUTE format('ALTER TABLE %I ALTER COLUMN id DROP DEFAULT', rec.table_name);
            EXCEPTION WHEN OTHERS THEN
                NULL;
            END;

            BEGIN
                EXECUTE format(
                    'ALTER TABLE %I ALTER COLUMN id TYPE uuid USING CASE WHEN id IS NULL THEN gen_random_uuid() ELSE gen_random_uuid() END',
                    rec.table_name
                );
            EXCEPTION WHEN OTHERS THEN
                NULL;
            END;

            BEGIN
                EXECUTE format('ALTER TABLE %I ALTER COLUMN id SET DEFAULT gen_random_uuid()', rec.table_name);
            EXCEPTION WHEN OTHERS THEN
                NULL;
            END;

            BEGIN
                EXECUTE format('ALTER TABLE %I ALTER COLUMN id SET NOT NULL', rec.table_name);
            EXCEPTION WHEN OTHERS THEN
                NULL;
            END;
        END LOOP;
    END $$;
    """
    with conn.cursor() as cur:
        cur.execute(ddl)


def _with_default_sslmode(database_url: str) -> str:
    parsed = urlparse(database_url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))

    host = (parsed.hostname or "").lower()
    is_local = host in {"localhost", "127.0.0.1"}
    if "sslmode" not in query and not is_local:
        query["sslmode"] = "require"

    return urlunparse(parsed._replace(query=urlencode(query)))


def get_database_url() -> str:
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL is not set. PostgreSQL is required for this deployment."
        )
    return _with_default_sslmode(database_url)


def get_pool() -> ThreadedConnectionPool:
    global _POOL
    if _POOL is not None:
        return _POOL

    with _LOCK:
        if _POOL is None:
            _POOL = ThreadedConnectionPool(
                minconn=int(os.getenv("DB_POOL_MIN", "1")),
                maxconn=int(os.getenv("DB_POOL_MAX", "20")),
                dsn=get_database_url(),
            )
            # Required for UUID PK defaults: gen_random_uuid()
            conn = _POOL.getconn()
            try:
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute('CREATE EXTENSION IF NOT EXISTS "pgcrypto";')
                _ensure_uuid_pk_defaults(conn)
            finally:
                _POOL.putconn(conn)
    return _POOL


def acquire_connection():
    return get_pool().getconn()


def release_connection(conn) -> None:
    get_pool().putconn(conn)
