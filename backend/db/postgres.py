import os
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
import atexit

# Global pool instance
_pool = None

def _get_pool():
    global _pool
    if _pool is None:
        db_url = os.getenv("DATABASE_URL")
        # Ensure we don't accidentally initialize without knowing the URL
        if db_url:
            _pool = ThreadedConnectionPool(
                minconn=1,
                maxconn=5,
                dsn=db_url,
                sslmode="require"
            )
    return _pool

def acquire_connection():
    pool = _get_pool()
    if pool is None:
        # Fallback if DATABASE_URL is somehow not set but called (unlikely in prod)
        return psycopg2.connect(
            os.getenv("DATABASE_URL"),
            sslmode="require",
        )
    return pool.getconn()

def release_connection(conn):
    pool = _get_pool()
    if pool is not None:
        try:
            # Always put back safely. We don't rollback here because the wrapper handles it if needed,
            # but usually it's safer to rollback in case a transaction was left open.
            conn.rollback()
        except:
            pass
        finally:
            pool.putconn(conn, close=False)
    else:
        conn.close()

# Ensure pool gets closed on app exit
@atexit.register
def _close_pool():
    global _pool
    if _pool is not None:
        _pool.closeall()
