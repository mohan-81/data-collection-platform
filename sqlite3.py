"""
SQLite compatibility shim backed by PostgreSQL.

This module intentionally exposes a sqlite3-like API used across the codebase,
while routing all DB operations to PostgreSQL using DATABASE_URL.
"""

from __future__ import annotations

import re
from typing import Iterable

import psycopg2
from psycopg2 import Error as _PGError
from psycopg2 import extras as _pg_extras

from backend.db.postgres import acquire_connection, release_connection

paramstyle = "qmark"
threadsafety = 2
apilevel = "2.0"


class Error(Exception):
    pass


class DatabaseError(Error):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class InterfaceError(DatabaseError):
    pass


class Row:
    pass


def _map_error(exc: Exception) -> Exception:
    if isinstance(exc, psycopg2.IntegrityError):
        return IntegrityError(str(exc))
    if isinstance(exc, psycopg2.ProgrammingError):
        return ProgrammingError(str(exc))
    if isinstance(exc, psycopg2.InterfaceError):
        return InterfaceError(str(exc))
    if isinstance(exc, _PGError):
        return OperationalError(str(exc))
    return exc


class _CompatRow:
    def __init__(self, columns: list[str], values: tuple):
        self._columns = columns
        self._values = tuple(values)
        self._index = {k: i for i, k in enumerate(columns)}

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]
        return self._values[self._index[key]]

    def __iter__(self):
        return iter(self._values)

    def __len__(self):
        return len(self._values)

    def keys(self):
        return list(self._columns)

    def items(self):
        return [(k, self._values[i]) for i, k in enumerate(self._columns)]

    def as_dict(self):
        return {k: self._values[i] for i, k in enumerate(self._columns)}


def _replace_qmark_params(sql: str) -> str:
    out = []
    in_single = False
    in_double = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        if ch == "'" and not in_double:
            in_single = not in_single
            out.append(ch)
        elif ch == '"' and not in_single:
            in_double = not in_double
            out.append(ch)
        elif ch == "?" and not in_single and not in_double:
            out.append("%s")
        else:
            out.append(ch)
        i += 1
    return "".join(out)


def _quote_reserved_user_identifier(sql: str) -> str:
    """
    Quote bare `user` identifier as `"user"` outside string/identifier quotes.
    This avoids PostgreSQL reserved keyword conflicts while preserving logic.
    """
    out = []
    token = []
    in_single = False
    in_double = False

    def flush_token():
        nonlocal token
        if not token:
            return
        t = "".join(token)
        if t.lower() == "user":
            out.append('"user"')
        else:
            out.append(t)
        token = []

    for ch in sql:
        if in_single:
            out.append(ch)
            if ch == "'":
                in_single = False
            continue

        if in_double:
            out.append(ch)
            if ch == '"':
                in_double = False
            continue

        if ch == "'":
            flush_token()
            in_single = True
            out.append(ch)
            continue

        if ch == '"':
            flush_token()
            in_double = True
            out.append(ch)
            continue

        if ch.isalnum() or ch == "_":
            token.append(ch)
        else:
            flush_token()
            out.append(ch)

    flush_token()
    return "".join(out)


def _split_columns(raw: str) -> list[str]:
    cols = []
    current = []
    depth = 0
    for ch in raw:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            cols.append("".join(current).strip().strip('"'))
            current = []
            continue
        current.append(ch)
    if current:
        cols.append("".join(current).strip().strip('"'))
    return [c for c in cols if c]


class Cursor:
    def __init__(self, conn: "Connection"):
        self._conn = conn
        self._cur = conn._raw.cursor()
        self._description = None
        self._last_rows = None
        self.rowcount = -1
        self.lastrowid = None

    @property
    def description(self):
        if self._description is not None:
            return self._description
        return self._cur.description

    def close(self):
        try:
            self._cur.close()
        except Exception:
            pass

    def _set_memory_result(self, columns: list[str], rows: list[tuple]):
        self._description = [(c, None, None, None, None, None, None) for c in columns]
        self._last_rows = rows
        self.rowcount = len(rows)

    def _clear_memory_result(self):
        self._description = None
        self._last_rows = None

    def _table_pk_columns(self, table_name: str) -> list[str]:
        q = """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a
              ON a.attrelid = i.indrelid
             AND a.attnum = ANY(i.indkey)
            JOIN pg_class c
              ON c.oid = i.indrelid
            JOIN pg_namespace n
              ON n.oid = c.relnamespace
            WHERE i.indisprimary = true
              AND c.relname = %s
              AND n.nspname = current_schema()
            ORDER BY array_position(i.indkey, a.attnum)
        """
        self._cur.execute(q, (table_name,))
        return [r[0] for r in self._cur.fetchall()]

    def _table_unique_columns(self, table_name: str) -> list[str]:
        q = """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a
              ON a.attrelid = i.indrelid
             AND a.attnum = ANY(i.indkey)
            JOIN pg_class c
              ON c.oid = i.indrelid
            JOIN pg_namespace n
              ON n.oid = c.relnamespace
            WHERE i.indisunique = true
              AND i.indisprimary = false
              AND c.relname = %s
              AND n.nspname = current_schema()
            ORDER BY i.indexrelid, array_position(i.indkey, a.attnum)
            LIMIT 10
        """
        self._cur.execute(q, (table_name,))
        rows = self._cur.fetchall()
        return [r[0] for r in rows]

    def _table_columns(self, table_name: str) -> list[str]:
        q = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = %s
            ORDER BY ordinal_position
        """
        self._cur.execute(q, (table_name,))
        return [r[0] for r in self._cur.fetchall()]

    def _rewrite_insert_or(self, sql: str) -> str:
        m = re.match(
            r"(?is)^\s*INSERT\s+OR\s+(REPLACE|IGNORE)\s+INTO\s+([a-zA-Z_][\w]*)\s*(\((.*?)\))?\s*VALUES\s*(\(.+\))\s*$",
            sql.strip(),
        )
        if not m:
            return sql

        mode = m.group(1).upper()
        table = m.group(2)
        has_cols = m.group(3) is not None
        raw_cols = m.group(4) or ""
        values_sql = m.group(5)

        cols = _split_columns(raw_cols) if has_cols else self._table_columns(table)
        base = f"INSERT INTO {table}"
        if has_cols:
            base += f" ({raw_cols})"
        base += f" VALUES {values_sql}"

        if mode == "IGNORE":
            return f"{base} ON CONFLICT DO NOTHING"

        conflict_cols = self._table_pk_columns(table)
        if not conflict_cols:
            conflict_cols = self._table_unique_columns(table)
        if not conflict_cols:
            return base

        updatable = [c for c in cols if c not in conflict_cols]
        if not updatable:
            return f"{base} ON CONFLICT ({', '.join(conflict_cols)}) DO NOTHING"

        set_clause = ", ".join([f"{c}=EXCLUDED.{c}" for c in updatable])
        return f"{base} ON CONFLICT ({', '.join(conflict_cols)}) DO UPDATE SET {set_clause}"

    def _rewrite_sql(self, sql: str) -> tuple[str | None, str | None]:
        s = sql.strip()

        pragma_tbl = re.match(r"(?is)^PRAGMA\s+table_info\s*\(\s*([^)]+)\s*\)\s*;?$", s)
        if pragma_tbl:
            table = pragma_tbl.group(1).strip().strip("'").strip('"')
            q = """
                WITH pk_cols AS (
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                     AND tc.table_schema = kcu.table_schema
                    WHERE tc.constraint_type = 'PRIMARY KEY'
                      AND tc.table_schema = current_schema()
                      AND tc.table_name = %s
                )
                SELECT
                    c.ordinal_position - 1 AS cid,
                    c.column_name AS name,
                    c.data_type AS type,
                    CASE WHEN c.is_nullable = 'NO' THEN 1 ELSE 0 END AS notnull,
                    c.column_default AS dflt_value,
                    CASE WHEN p.column_name IS NULL THEN 0 ELSE 1 END AS pk
                FROM information_schema.columns c
                LEFT JOIN pk_cols p
                  ON p.column_name = c.column_name
                WHERE c.table_schema = current_schema()
                  AND c.table_name = %s
                ORDER BY c.ordinal_position
            """
            self._cur.execute(q, (table, table))
            self._set_memory_result(
                ["cid", "name", "type", "notnull", "dflt_value", "pk"],
                self._cur.fetchall(),
            )
            return None, "memory"

        if re.match(r"(?is)^PRAGMA\s+(journal_mode|busy_timeout|synchronous)", s):
            self._set_memory_result(["ok"], [])
            return None, "memory"

        out = sql
        out = _quote_reserved_user_identifier(out)
        out = re.sub(
            r"(?is)\bid\s+INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b",
            "id UUID PRIMARY KEY DEFAULT gen_random_uuid()",
            out,
        )
        out = re.sub(
            r"(?is)\bid\s+INT\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b",
            "id UUID PRIMARY KEY DEFAULT gen_random_uuid()",
            out,
        )
        out = re.sub(
            r"(?is)\bid\s+INTEGER\s+PRIMARY\s+KEY\b",
            "id UUID PRIMARY KEY DEFAULT gen_random_uuid()",
            out,
        )
        out = re.sub(
            r"(?is)\bid\s+INT\s+PRIMARY\s+KEY\b",
            "id UUID PRIMARY KEY DEFAULT gen_random_uuid()",
            out,
        )
        out = re.sub(r"(?is)\bINTEGER\s+AUTOINCREMENT\b", "SERIAL", out)
        out = re.sub(r"(?is)\bAUTOINCREMENT\b", "", out)
        out = re.sub(r"(?is)\bDATETIME\b(?!\s*\()", "TIMESTAMP", out)
        out = re.sub(r"(?is)datetime\s*\(\s*'now'\s*\)", "NOW()", out)
        out = re.sub(r"(?is)datetime\s*\(\s*([a-zA-Z_][\w\.]*)\s*\)", r"(\1)::timestamp", out)
        out = re.sub(r"(?is)strftime\s*\(\s*'%Y-%m'\s*,\s*([^)]+)\)", r"to_char(\1, 'YYYY-MM')", out)
        out = re.sub(r"(?is)VALUES\s*\(\s*NULL\s*,", "VALUES (DEFAULT,", out)
        out = self._rewrite_insert_or(out)
        out = _replace_qmark_params(out)

        return out, "db"

    def execute(self, sql: str, parameters: Iterable | None = None):
        try:
            self._clear_memory_result()
            rewritten, mode = self._rewrite_sql(sql)
            if mode == "memory":
                return self

            params = tuple(parameters) if parameters is not None else None
            self._cur.execute(rewritten, params)
            self.rowcount = self._cur.rowcount
            if self._cur.description:
                self._description = self._cur.description
            try:
                if sql.strip().upper().startswith("INSERT"):
                    self._cur.execute("SELECT LASTVAL()")
                    v = self._cur.fetchone()
                    self.lastrowid = v[0] if v else None
            except Exception:
                self.lastrowid = None
            return self
        except Exception as exc:
            raise _map_error(exc)

    def executemany(self, sql: str, seq_of_parameters):
        try:
            self._clear_memory_result()
            rewritten, mode = self._rewrite_sql(sql)
            if mode == "memory":
                return self
            self._cur.executemany(rewritten, list(seq_of_parameters))
            self.rowcount = self._cur.rowcount
            return self
        except Exception as exc:
            raise _map_error(exc)

    def executescript(self, script: str):
        for stmt in [s.strip() for s in script.split(";") if s.strip()]:
            self.execute(stmt)
        return self

    def _convert_row(self, row):
        if row is None:
            return None
        if self._conn.row_factory is Row:
            cols = [d[0] for d in (self.description or [])]
            return _CompatRow(cols, row)
        return row

    def fetchone(self):
        if self._last_rows is not None:
            if not self._last_rows:
                return None
            row = self._last_rows.pop(0)
            return self._convert_row(row)
        row = self._cur.fetchone()
        return self._convert_row(row)

    def fetchall(self):
        if self._last_rows is not None:
            rows = self._last_rows
            self._last_rows = []
            return [self._convert_row(r) for r in rows]
        rows = self._cur.fetchall()
        return [self._convert_row(r) for r in rows]

    def fetchmany(self, size=None):
        if self._last_rows is not None:
            size = size or 1
            rows = self._last_rows[:size]
            self._last_rows = self._last_rows[size:]
            return [self._convert_row(r) for r in rows]
        rows = self._cur.fetchmany(size)
        return [self._convert_row(r) for r in rows]


class Connection:
    def __init__(self, isolation_level=None):
        self._raw = acquire_connection()
        self._closed = False
        self.row_factory = None
        self.isolation_level = isolation_level
        self._raw.autocommit = isolation_level is None

    def cursor(self):
        return Cursor(self)

    def execute(self, sql, parameters=None):
        cur = self.cursor()
        cur.execute(sql, parameters)
        return cur

    def executemany(self, sql, seq_of_parameters):
        cur = self.cursor()
        cur.executemany(sql, seq_of_parameters)
        return cur

    def commit(self):
        try:
            self._raw.commit()
        except Exception as exc:
            raise _map_error(exc)

    def rollback(self):
        try:
            self._raw.rollback()
        except Exception as exc:
            raise _map_error(exc)

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            release_connection(self._raw)
        except Exception as exc:
            raise _map_error(exc)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None and not self._raw.autocommit:
                self.commit()
            elif exc_type is not None and not self._raw.autocommit:
                self.rollback()
        finally:
            self.close()
        return False


def connect(
    database=None,
    timeout=None,
    detect_types=None,
    isolation_level=None,
    check_same_thread=None,
    factory=None,
    cached_statements=None,
    uri=None,
):
    _ = (database, timeout, detect_types, check_same_thread, factory, cached_statements, uri)
    return Connection(isolation_level=isolation_level)
