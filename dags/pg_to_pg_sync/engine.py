from __future__ import annotations

import logging
import time
from datetime import datetime, date, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Tuple

from pg_to_pg_sync.TableConfig import TableConfig

import json  # <-- added
import psycopg2
import psycopg2.extras as extras
import psycopg2.extensions

# Module-level logger for helpers
LOG = logging.getLogger(__name__)

# ============================== Helpers (module-level; stateless) ===============================

def _json_sanitize(value: Any) -> Any:
    """
    Ensure value is JSON-serializable (safe for Airflow XCom push).
    - Converts datetime/date/Decimal and other exotic types via default=str.
    - Round-trips through JSON to guarantee primitives only.
    """
    return json.loads(json.dumps(value, default=str))

def _fq_table(schema: str, table: str) -> str:
    fq = f"{schema}.{table}"
    if LOG.isEnabledFor(logging.DEBUG):
        LOG.debug("FQ table: %s", fq)
    return fq

def _qi(ident: str) -> str:
    q = f'"{ident}"'
    if LOG.isEnabledFor(logging.DEBUG):
        LOG.debug("Quoted identifier: raw=%r quoted=%r", ident, q)
    return q

def _pk_cols_list(pk: str | None) -> List[str]:
    cols = [] if not pk else [c.strip() for c in pk.split(",") if c.strip()]
    if LOG.isEnabledFor(logging.DEBUG):
        LOG.debug("Primary key cols parsed: %s", cols)
    return cols

def _is_sequence_default(default_expr: str | None) -> bool:
    res = bool(default_expr) and "nextval(" in default_expr.lower()
    if LOG.isEnabledFor(logging.DEBUG):
        LOG.debug("Is sequence default? %r -> %s", default_expr, res)
    return res

def _is_constant_default(default_expr: str | None) -> bool:
    if not default_expr:
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("Constant default? %r -> False (no default)", default_expr)
        return False
    d = default_expr.strip().lower()
    if any(func in d for func in ("now()", "clock_timestamp()", "current_timestamp", "nextval(", "uuid_generate_v4", "gen_random_uuid")):
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("Constant default? %r -> False (volatile func)", default_expr)
        return False
    if d in ("null", "true", "false"):
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("Constant default? %r -> True (literal)", default_expr)
        return True
    if d.startswith("'"):  # quoted string
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("Constant default? %r -> True (quoted string)", default_expr)
        return True
    if d[0].isdigit():
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("Constant default? %r -> True (starts with digit)", default_expr)
        return True
    if "'::" in d or "::" in d:
        res = "(" not in d
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("Constant default? %r -> %s (cast, parens=%s)", default_expr, res, "(" in d)
        return res
    try:
        float(d)
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("Constant default? %r -> True (floatable)", default_expr)
        return True
    except Exception:
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("Constant default? %r -> False (fallback)", default_expr)
        return False

def _row_count(conn, schema: str, table: str) -> int:
    t0 = time.perf_counter()
    with conn.cursor() as c:
        LOG.info("Checking existence for %s.%s", schema, table)
        c.execute(
            """
            SELECT EXISTS (
              SELECT 1 FROM information_schema.tables
              WHERE table_schema=%s AND table_name=%s
            )
            """,
            (schema, table),
        )
        exists = c.fetchone()[0]
        if not exists:
            LOG.info("Table %s.%s does not exist; row count = 0", schema, table)
            return 0
        fq = _fq_table(schema, table)
        LOG.info("Counting rows in %s", fq)
        c.execute(f"SELECT COUNT(*) FROM {fq}")
        cnt = int(c.fetchone()[0])
        LOG.info("Row count for %s: %d (%.3fs)", fq, cnt, time.perf_counter() - t0)
        return cnt

def _get_columns(conn, schema: str, table: str) -> List[str]:
    t0 = time.perf_counter()
    LOG.debug("Fetching column list for %s.%s", schema, table)
    with conn.cursor() as c:
        c.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table),
        )
        cols = [r[0] for r in c.fetchall()]
    LOG.info("Columns for %s.%s: %s (%.3fs)", schema, table, cols, time.perf_counter() - t0)
    return cols

def _build_insert_values_sql(schema: str, table: str, cols: List[str]) -> str:
    col_list = ", ".join(_qi(c) for c in cols)
    sql = f"INSERT INTO {_fq_table(schema, table)} ({col_list}) VALUES %s"
    if LOG.isEnabledFor(logging.DEBUG):
        LOG.debug("Generated INSERT SQL: %s", sql)
    return sql

def _build_upsert_values_sql(schema: str, table: str, cols: List[str], pk_cols: List[str]) -> str:
    col_list = ", ".join(_qi(c) for c in cols)
    pk_list = ", ".join(_qi(c) for c in pk_cols)
    set_list = ", ".join(f"{_qi(c)} = EXCLUDED.{_qi(c)}" for c in cols if c not in pk_cols)
    sql = (
        f"INSERT INTO {_fq_table(schema, table)} ({col_list}) VALUES %s "
        f"ON CONFLICT ({pk_list}) DO UPDATE SET {set_list}"
    )
    if LOG.isEnabledFor(logging.DEBUG):
        LOG.debug("Generated UPSERT SQL: %s", sql)
    return sql

def _build_simple_insert (schema: str, table: str, cols: List[str], pk_cols: List[str]) -> str:
    col_list = ", ".join(_qi(c) for c in cols)
    pk_list = ", ".join(_qi(c) for c in pk_cols)
    set_list = ", ".join(f"{_qi(c)} = EXCLUDED.{_qi(c)}" for c in cols if c not in pk_cols)
    sql = (
        f"INSERT INTO {_fq_table(schema, table)} ({col_list}) VALUES %s "    )
    if LOG.isEnabledFor(logging.DEBUG):
        LOG.debug("Generated SQL: %s", sql)
    return sql

def _build_table_hash_sql(schema: str, table: str, columns: List[str], order_cols: List[str]) -> str:
    row_expr = "md5(" + " || '§' || ".join(f"COALESCE({_qi(c)}::text,'')" for c in columns) + ")"
    if order_cols:
        order_by = ", ".join(_qi(c) for c in order_cols)
        sql = f"SELECT md5(string_agg({row_expr}, '|' ORDER BY {order_by})) FROM {_fq_table(schema, table)}"
    else:
        sql = f"SELECT md5(string_agg({row_expr}, '|' ORDER BY {row_expr})) FROM {_fq_table(schema, table)}"
    if LOG.isEnabledFor(logging.DEBUG):
        LOG.debug("Generated table-hash SQL for %s.%s: %s", schema, table, sql)
    return sql

def _count_and_hash(conn, schema: str, table: str, columns: List[str], order_cols: List[str]) -> Tuple[int, str | None]:
    t0 = time.perf_counter()
    fq = _fq_table(schema, table)
    with conn.cursor() as c:
        LOG.debug("Counting rows for hash precheck on %s", fq)
        c.execute(f"SELECT COUNT(*) FROM {fq}")
        cnt = int(c.fetchone()[0])
        if cnt == 0 or not columns:
            LOG.info("Count+hash for %s -> count=%d, hash=None (no rows or no columns) (%.3fs)", fq, cnt, time.perf_counter() - t0)
            return cnt, None
        sql = _build_table_hash_sql(schema, table, columns, order_cols)
        LOG.debug("Executing hash SQL on %s", fq)
        c.execute(sql)
        h = c.fetchone()[0]
        LOG.info("Count+hash for %s -> count=%d, hash=%s (%.3fs)", fq, cnt, h, time.perf_counter() - t0)
        return cnt, h

def _pg_col_type(conn, schema: str, table: str, column: str) -> str:
    LOG.debug("Fetching PostgreSQL type for %s.%s.%s", schema, table, column)
    with conn.cursor() as c:
        c.execute(
            """
            SELECT data_type
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s AND column_name=%s
            """,
            (schema, table, column),
        )
        row = c.fetchone()
        t = (row[0].lower() if row and row[0] else "")
        LOG.info("Column type for %s.%s.%s -> %r", schema, table, column, t)
        return t

def _default_for_type(pg_type: str):
    t = (pg_type or "").lower()
    if t in ("bigint", "integer", "smallint"):
        d = 0
    elif t in ("numeric", "decimal", "real", "double precision"):
        d = Decimal("0")
    elif t == "date":
        d = date(1970, 1, 1)
    elif t == "timestamp with time zone":
        d = datetime(1970, 1, 1, tzinfo=timezone.utc)
    elif t == "timestamp without time zone":
        d = datetime(1970, 1, 1)
    else:
        d = 0
    if LOG.isEnabledFor(logging.DEBUG):
        LOG.debug("Default for pg_type %r -> %r", pg_type, d)
    return d

def _dst_pk_types(conn, schema: str, table: str, pk_cols: list[str]) -> list[tuple[str, str]]:
    LOG.debug("Fetching PK types for %s.%s, pk_cols=%s", schema, table, pk_cols)
    with conn.cursor() as c:
        c.execute(
            """
            SELECT c.column_name, c.udt_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema     = kcu.table_schema
            JOIN information_schema.columns c
              ON c.table_schema = kcu.table_schema
             AND c.table_name   = kcu.table_name
             AND c.column_name  = kcu.column_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = %s
              AND tc.table_name   = %s
            ORDER BY kcu.ordinal_position
            """,
            (schema, table),
        )
        rows = c.fetchall()
        tmap = {r[0]: r[1] for r in rows}
        result = [(col, tmap[col]) for col in pk_cols if col in tmap]
        LOG.info("PK types for %s.%s -> %s", schema, table, result)
        return result

# ============================== Engine (single class) ===============================

class SyncDataEngine:
    def __init__(self, logger: logging.Logger | None = None):
        self.log = logger or logging.getLogger(__name__)
        self.log.debug("SyncDataEngine initialized with logger=%r", self.log.name)

    # ------------------------ Schema alignment ------------------------

    def ensure_table(self, cfg: TableConfig, src, dst) -> Dict[str, Any]:
        """
        • Creates the destination table on first run (schema + PK if configured).
        • On subsequent runs, compares source→dest schemas and adds any missing columns
        (safe handling for volatile vs constant defaults).
        • Ensures a PRIMARY KEY exists when cfg.primary_key is set:
            - Refuses NULLs in PK columns,
            - Auto-deduplicates duplicate keys via DISTINCT ON, then adds PK.
        """
        self.log.info(
            "ensure_table: src=%s.%s dst=%s.%s pk=%s",
            cfg.pg_schema, cfg.pg_table, cfg.dst_schema, cfg.dst_table, cfg.primary_key
        )
        self.log.info("has_hard_delete=%s", cfg.has_hard_delete)
        t0 = time.perf_counter()
        src_schema, src_table = cfg.pg_schema, cfg.pg_table
        dst_schema, dst_table = cfg.dst_schema, cfg.dst_table
        pk_cols = _pk_cols_list(cfg.primary_key)

        # ---- 1) source column metadata (ordered) ----
        self.log.debug("Loading source column metadata from %s.%s", src_schema, src_table)
        with src.cursor(cursor_factory=psycopg2.extras.DictCursor) as c:
            c.execute(
                """
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema=%s AND table_name=%s
                ORDER BY ordinal_position
                """,
                (src_schema, src_table),
            )
            src_cols = c.fetchall()
            self.log.info("Discovered %d source columns for %s.%s", len(src_cols), src_schema, src_table)

        missing_added: list[str] = []
        pk_added = False
        dedup_performed = False

        with dst.cursor() as d:
            # ---- 2) does destination table already exist? ----
            self.log.debug("Checking if destination table %s.%s exists", dst_schema, dst_table)
            d.execute(
                """
                SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema=%s AND table_name=%s
                )
                """,
                (dst_schema, dst_table),
            )
            table_exists = bool(d.fetchone()[0])
            self.log.info("Destination table exists? %s", table_exists)

            # ---- 3) If table doesn't exist: create schema + table (+PK) ----
            if not table_exists:
                self.log.info("Creating schema (if missing): %s", dst_schema)
                d.execute(f"CREATE SCHEMA IF NOT EXISTS {dst_schema};")

                self.log.info("Creating destination table %s.%s", dst_schema, dst_table)
                col_defs = []
                for col in src_cols:
                    parts = [f'{_qi(col["column_name"])} {col["data_type"]}']
                    default_expr = col["column_default"]
                    if default_expr and not _is_sequence_default(default_expr):
                        parts.append(f"DEFAULT {default_expr}")
                    if col["is_nullable"] == "NO":
                        parts.append("NOT NULL")
                    cd = " ".join(parts)
                    col_defs.append(cd)
                    self.log.debug("Column definition for create: %s", cd)

                pk_clause = f", PRIMARY KEY ({', '.join(_qi(c) for c in pk_cols)})" if pk_cols else ""
                create_sql = (
                    f"CREATE TABLE {_fq_table(dst_schema, dst_table)} "
                    f"({', '.join(col_defs)}{pk_clause});"
                )
                self.log.debug("CREATE TABLE SQL: %s", create_sql)
                d.execute(create_sql)
                dst.commit()
                self.log.info("✅ Created table %s", _fq_table(dst_schema, dst_table))

            # ---- 4) Add missing columns (safe backfill for constant defaults) ----
            self.log.debug("Fetching existing dest columns for %s.%s", dst_schema, dst_table)
            d.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema=%s AND table_name=%s
                """,
                (dst_schema, dst_table),
            )
            dst_cols = {r[0] for r in d.fetchall()}
            self.log.info("Destination currently has %d columns", len(dst_cols))

            d.execute(f"SELECT EXISTS (SELECT 1 FROM {_fq_table(dst_schema, dst_table)} LIMIT 1)")
            has_rows = bool(d.fetchone()[0])
            self.log.info("Destination table %s has existing rows? %s", _fq_table(dst_schema, dst_table), has_rows)

            # compute missing
            missing = [c for c in src_cols if c["column_name"] not in dst_cols]
            self.log.info("Missing columns to add: %d -> %s", len(missing), [c["column_name"] for c in missing])

            for col in missing:
                col_name = col["column_name"]
                data_type = col["data_type"]
                default_expr = col["column_default"]
                not_null = (col["is_nullable"] == "NO")

                fq = _fq_table(dst_schema, dst_table)
                self.log.debug(
                    "Adding column %s %s default=%r not_null=%s (has_rows=%s)",
                    col_name, data_type, default_expr, not_null, has_rows
                )

                if not has_rows:
                    parts = [f"ALTER TABLE {fq} ADD COLUMN {_qi(col_name)} {data_type}"]
                    if default_expr and not _is_sequence_default(default_expr):
                        parts.append(f"DEFAULT {default_expr}")
                    if not_null:
                        parts.append("NOT NULL")
                    sql = " ".join(parts)
                    self.log.debug("ALTER (no rows) SQL: %s", sql)
                    d.execute(sql)
                else:
                    sql_add = f"ALTER TABLE {fq} ADD COLUMN {_qi(col_name)} {data_type}"
                    self.log.debug("ALTER (has rows) ADD COLUMN SQL: %s", sql_add)
                    d.execute(sql_add)
                    if _is_constant_default(default_expr):
                        sql_def = f"ALTER TABLE {fq} ALTER COLUMN {_qi(col_name)} SET DEFAULT {default_expr}"
                        sql_fill = f"UPDATE {fq} SET {_qi(col_name)} = DEFAULT WHERE {_qi(col_name)} IS NULL"
                        self.log.debug("Backfill constant default SQLs: %s | %s", sql_def, sql_fill)
                        d.execute(sql_def)
                        d.execute(sql_fill)
                        if not_null:
                            sql_nn = f"ALTER TABLE {fq} ALTER COLUMN {_qi(col_name)} SET NOT NULL"
                            self.log.debug("Enforce NOT NULL SQL: %s", sql_nn)
                            d.execute(sql_nn)
                missing_added.append(col_name)

            if missing:
                dst.commit()
                self.log.info(
                    "🆕 Added %s new column(s) to %s: %s",
                    len(missing),
                    _fq_table(dst_schema, dst_table),
                    ", ".join(c["column_name"] for c in missing),
                )

            # ---- 5) Ensure PRIMARY KEY exists (when configured) ----
            if pk_cols:
                self.log.debug("Checking if PRIMARY KEY exists on %s.%s", dst_schema, dst_table)
                d.execute(
                    """
                    SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.table_constraints tc
                    WHERE tc.table_schema=%s AND tc.table_name=%s
                        AND tc.constraint_type='PRIMARY KEY'
                    )
                    """,
                    (dst_schema, dst_table),
                )
                pk_exists = bool(d.fetchone()[0])
                self.log.info("PRIMARY KEY exists on %s.%s ? %s", dst_schema, dst_table, pk_exists)

                if not pk_exists:
                    fq = _fq_table(dst_schema, dst_table)
                    self.log.info("Preparing to add PRIMARY KEY on %s (%s)", fq, pk_cols)
                    # Verify PK columns exist on dest
                    d.execute(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema=%s AND table_name=%s
                        """,
                        (dst_schema, dst_table),
                    )
                    have_cols = {r[0] for r in d.fetchall()}
                    missing_pk_cols = [c for c in pk_cols if c not in have_cols]
                    if missing_pk_cols:
                        self.log.error("Missing PK columns on %s: %s", fq, missing_pk_cols)
                        raise RuntimeError(
                            f"Cannot create PK on {fq}; missing columns: {', '.join(missing_pk_cols)}"
                        )

                    # Disallow NULLs in PK columns
                    null_pred = " OR ".join(f"{_qi(c)} IS NULL" for c in pk_cols)
                    chk_null_sql = f"SELECT COUNT(*) FROM {fq} WHERE {null_pred}"
                    self.log.debug("Checking NULLs in PK columns with: %s", chk_null_sql)
                    d.execute(chk_null_sql)
                    nulls = int(d.fetchone()[0])
                    if nulls > 0:
                        self.log.error("Found %d NULL(s) in PK columns %s on %s", nulls, pk_cols, fq)
                        raise RuntimeError(
                            f"Cannot add PRIMARY KEY to {fq}; {nulls} row(s) have NULL in PK columns ({', '.join(pk_cols)})."
                        )

                    # Check duplicates of PK combination
                    pk_cols_quoted = ", ".join(_qi(c) for c in pk_cols)
                    dup_sql = (
                        f"SELECT 1 FROM {fq} GROUP BY {pk_cols_quoted} HAVING COUNT(*) > 1 LIMIT 1"
                    )
                    self.log.debug("Checking duplicate PK combinations with: %s", dup_sql)
                    d.execute(dup_sql)
                    have_dups = bool(d.fetchone())

                    if have_dups:
                        # Auto-deduplicate by keeping first arbitrary row per key
                        self.log.warning(
                            "⚠️ Duplicate PK combinations found on %s; deduplicating via DISTINCT ON (%s)",
                            fq, pk_cols_quoted,
                        )
                        dedup_sql = (
                            f"""
                            CREATE TEMP TABLE tmp_{dst_table} AS
                            SELECT DISTINCT ON ({pk_cols_quoted}) *
                            FROM {fq}
                            ORDER BY {pk_cols_quoted};
                            TRUNCATE {fq};
                            INSERT INTO {fq} SELECT * FROM tmp_{dst_table};
                            DROP TABLE tmp_{dst_table};
                            """
                        )
                        self.log.debug("Deduplication SQL: %s", dedup_sql)
                        d.execute(dedup_sql)
                        dst.commit()
                        dedup_performed = True
                        self.log.info("Deduplication committed on %s", fq)

                    # Finally, add the PRIMARY KEY constraint
                    constraint_name = f"{dst_table}_pk"
                    add_pk_sql = f'ALTER TABLE {fq} ADD CONSTRAINT {_qi(constraint_name)} PRIMARY KEY ({pk_cols_quoted});'
                    self.log.debug("Add PK SQL: %s", add_pk_sql)
                    d.execute(add_pk_sql)
                    dst.commit()
                    pk_added = True
                    self.log.info("✅ Added primary key to %s", fq)
                else:
                    self.log.info("ℹ️ Table %s already has a primary key", _fq_table(dst_schema, dst_table))

        result = {
            "dst_exists": True,
            "missing_added": missing_added,
            "pk_added": pk_added,
            "dedup_performed": dedup_performed,
            "elapsed": round(time.perf_counter() - t0, 3),
        }
        self.log.info("ensure_table result: %s", result)
        return _json_sanitize(result)  # <-- make XCom-safe

    # ------------------------ One sync pass ------------------------

    def sync_once(self, cfg: TableConfig, src, dst) -> Dict[str, Any]:
        """
        Run one sync pass according to cfg.sync_method.
        Returns a status dict with rows_synced, batches, decisions, timings.
        """
        self.log.info(
            "sync_once: method=%s src=%s.%s dst=%s.%s batch_size=%d pk=%s",
            cfg.sync_method, cfg.pg_schema, cfg.pg_table, cfg.dst_schema, cfg.dst_table, cfg.batch_size, cfg.primary_key
        )
        t0_sync = time.perf_counter()
        rows_synced = 0
        batches = 0
        pk_cols = _pk_cols_list(cfg.primary_key)
        method = cfg.sync_method

        try:
            self.log.debug("Fetching source columns for sync")
            columns_src = _get_columns(src, cfg.pg_schema, cfg.pg_table)
            col_list_sql = ", ".join(_qi(c) for c in columns_src)
            self.log.debug("Column projection for sync: %s", col_list_sql)

            if method == "full_replace":
                self.log.info("Starting full_replace precheck (counts, hashes)")
                # precheck: counts -> (if equal) hashes
                try:
                    src_cnt, _ = _count_and_hash(src, cfg.pg_schema, cfg.pg_table, [], [])
                    dst_cnt, _ = _count_and_hash(dst, cfg.dst_schema, cfg.dst_table, [], [])
                    self.log.info("Precheck counts: src=%d dst=%d", src_cnt, dst_cnt)
                    proceed_reload = False
                    if src_cnt != dst_cnt:
                        proceed_reload = True
                        self.log.debug("Proceed reload: counts differ")
                    elif src_cnt == 0:
                        proceed_reload = False
                        self.log.debug("Proceed reload: src is empty -> skip")
                    else:
                        order_cols = pk_cols if pk_cols else []
                        canonical_columns = sorted(columns_src)
                        self.log.debug("Computing hashes for equality check; order_cols=%s", order_cols)
                        _, src_hash = _count_and_hash(src, cfg.pg_schema, cfg.pg_table, canonical_columns, order_cols)
                        _, dst_hash = _count_and_hash(dst, cfg.dst_schema, cfg.dst_table, canonical_columns, order_cols)
                        proceed_reload = (src_hash != dst_hash)
                        self.log.info("Hash compare: src_hash=%s dst_hash=%s -> proceed=%s", src_hash, dst_hash, proceed_reload)
                    if not proceed_reload:
                        src.commit(); dst.commit()
                        result = {
                            "mode": "full_replace", "skipped": True, "rows_synced": 0,
                            "elapsed": round(time.perf_counter() - t0_sync, 3)
                        }
                        self.log.info("full_replace skipped, result=%s", result)
                        return _json_sanitize(result)  # <-- safe early return
                    src.commit(); dst.commit()
                    self.log.debug("Precheck completed; proceeding with reload")
                except Exception:
                    src.rollback(); dst.rollback()
                    self.log.warning("full_replace precheck failed; proceeding with reload", exc_info=True)

                live_fq = _fq_table(cfg.dst_schema, cfg.dst_table)
                with dst.cursor() as d:
                    try:
                        self.log.info("Attempting TRUNCATE on %s ...", live_fq)
                        d.execute(f"TRUNCATE TABLE {live_fq} RESTART IDENTITY;")
                        dst.commit()
                        self.log.info("TRUNCATE succeeded on %s", live_fq)
                    except Exception:
                        logging.warning("TRUNCATE failed, falling back to DELETE", exc_info=True)
                        # fallback commented intentionally as original

                self.log.info("Streaming source rows for full replace...")
                src_cur = src.cursor(name="pg2pg_stream_full")
                src_cur.itersize = cfg.batch_size
                src_cur.execute(f"SELECT {col_list_sql} FROM {_fq_table(cfg.pg_schema, cfg.pg_table)}")

                if pk_cols:
                    insert_sql = _build_insert_values_sql(cfg.dst_schema, cfg.dst_table, columns_src)
                else:
                    self.log.warning(
                        "No primary key configured for full_replace; using simple INSERT query."
                        " This may lead to duplicate rows if the source has duplicates.")
                    insert_sql = _build_simple_insert(cfg.dst_schema, cfg.dst_table, columns_src, pk_cols)

                self.log.debug("Insert SQL for full replace: %s", insert_sql)
                page: List[Tuple] = []
                with dst.cursor() as d:
                    d.execute("SET LOCAL synchronous_commit TO OFF")
                    for row in src_cur:
                        page.append(tuple(row))
                        if len(page) >= cfg.batch_size:
                            self.log.debug("Inserting batch of %d rows into %s.%s", len(page), cfg.dst_schema, cfg.dst_table)
                            t_batch = time.perf_counter()
                            extras.execute_values(d, insert_sql, page, page_size=cfg.batch_size)
                            dst.commit(); rows_synced += len(page); batches += 1; page.clear()
                            self.log.info("Batch committed (batches=%d, rows_synced=%d, took %.3fs)", batches, rows_synced, time.perf_counter() - t_batch)
                    if page:
                        self.log.debug("Inserting final batch of %d rows", len(page))
                        t_batch = time.perf_counter()
                        extras.execute_values(d, insert_sql, page, page_size=cfg.batch_size)
                        dst.commit(); rows_synced += len(page); batches += 1
                        self.log.info("Final batch committed (batches=%d, rows_synced=%d, took %.3fs)", batches, rows_synced, time.perf_counter() - t_batch)

            elif method == "upsert_time":
                inc_col = cfg.incremental_column
                self.log.info("Starting upsert_time with incremental_column=%r", inc_col)
                assert inc_col, "incremental_column must be set for upsert_time"
                if not pk_cols:
                    self.log.info("upsert_time without PK: ON CONFLICT cannot be used.")

                src_inc_type = _pg_col_type(src, cfg.pg_schema, cfg.pg_table, inc_col)
                self.log.debug("Incremental column %r type: %s", inc_col, src_inc_type)

                try:
                    src_cnt, _ = _count_and_hash(src, cfg.pg_schema, cfg.pg_table, [], [])
                    dst_cnt, _ = _count_and_hash(dst, cfg.dst_schema, cfg.dst_table, [], [])
                    self.log.info("Counts for upsert_time: src=%d dst=%d", src_cnt, dst_cnt)
                    with src.cursor() as c:
                        c.execute(f"SELECT MAX({_qi(inc_col)}) FROM {_fq_table(cfg.pg_schema, cfg.pg_table)}")
                        src_max = c.fetchone()[0]
                    with dst.cursor() as c:
                        c.execute(f"SELECT MAX({_qi(inc_col)}) FROM {_fq_table(cfg.dst_schema, cfg.dst_table)}")
                        dst_max = c.fetchone()[0]
                    if src_max is None: src_max = _default_for_type(src_inc_type)
                    if dst_max is None: dst_max = _default_for_type(src_inc_type)
                    self.log.info("Watermarks: src_max=%r dst_max=%r", src_max, dst_max)

                    equal_counts_and_max = (src_cnt == dst_cnt) and (src_max == dst_max)
                    self.log.debug("equal_counts_and_max=%s", equal_counts_and_max)
                    if equal_counts_and_max:
                        order_cols = pk_cols
                        canonical_columns = sorted(columns_src)
                        self.log.debug("Hashes for equality check (order_cols=%s)", order_cols)
                        _, src_hash = _count_and_hash(src, cfg.pg_schema, cfg.pg_table, canonical_columns, order_cols)
                        _, dst_hash = _count_and_hash(dst, cfg.dst_schema, cfg.dst_table, canonical_columns, order_cols)
                        if src_hash == dst_hash:
                            src.commit(); dst.commit()
                            result = {
                                "mode": "upsert_time", "skipped": True, "rows_synced": 0,
                                "elapsed": round(time.perf_counter() - t0_sync, 3)
                            }
                            self.log.info("upsert_time skipped (hash-equal), result=%s", result)
                            return _json_sanitize(result)  # <-- safe early return
                        else:
                            # Fallback: data differs even though counts and watermark match → upsert all
                            self.log.warning(
                                "Hashes differ despite equal counts and watermark; running upsert-all fallback"
                            )
                            src_cur = src.cursor(name="pg2pg_resync_all")
                            src_cur.itersize = cfg.batch_size
                            src_cur.execute(f"SELECT {col_list_sql} FROM {_fq_table(cfg.pg_schema, cfg.pg_table)}")

                            if pk_cols:
                                write_sql = _build_upsert_values_sql(cfg.dst_schema, cfg.dst_table, columns_src, pk_cols)
                                page: List[Tuple] = []
                                with dst.cursor() as d:
                                    d.execute("SET LOCAL synchronous_commit TO OFF")
                                    for row in src_cur:
                                        page.append(tuple(row))
                                        if len(page) >= cfg.batch_size:
                                            t_batch = time.perf_counter()
                                            extras.execute_values(d, write_sql, page, page_size=cfg.batch_size)
                                            dst.commit(); rows_synced += len(page); batches += 1; page.clear()
                                            self.log.info(
                                                "Upsert-all batch committed (batches=%d, rows_synced=%d, took %.3fs)",
                                                batches, rows_synced, time.perf_counter() - t_batch
                                            )
                                    if page:
                                        t_batch = time.perf_counter()
                                        extras.execute_values(d, write_sql, page, page_size=cfg.batch_size)
                                        dst.commit(); rows_synced += len(page); batches += 1
                                        self.log.info(
                                            "Final upsert-all batch committed (batches=%d, rows_synced=%d, took %.3fs)",
                                            batches, rows_synced, time.perf_counter() - t_batch
                                        )
                                src.commit()
                            else:
                                # Without PK, the only safe way to realign is a full reload
                                live_fq = _fq_table(cfg.dst_schema, cfg.dst_table)
                                with dst.cursor() as d:
                                    try:
                                        self.log.info("Attempting TRUNCATE on %s before reload ...", live_fq)
                                        d.execute(f"TRUNCATE TABLE {live_fq} RESTART IDENTITY;")
                                        dst.commit()
                                        self.log.info("TRUNCATE succeeded on %s", live_fq)
                                    except Exception:
                                        logging.warning("TRUNCATE failed, falling back to DELETE", exc_info=True)
                                        # Not performing DELETE fallback to avoid long-running locks by default

                                insert_sql = _build_simple_insert(cfg.dst_schema, cfg.dst_table, columns_src, pk_cols)
                                page: List[Tuple] = []
                                with dst.cursor() as d:
                                    d.execute("SET LOCAL synchronous_commit TO OFF")
                                    for row in src_cur:
                                        page.append(tuple(row))
                                        if len(page) >= cfg.batch_size:
                                            t_batch = time.perf_counter()
                                            extras.execute_values(d, insert_sql, page, page_size=cfg.batch_size)
                                            dst.commit(); rows_synced += len(page); batches += 1; page.clear()
                                            self.log.info(
                                                "Reload batch committed (batches=%d, rows_synced=%d, took %.3fs)",
                                                batches, rows_synced, time.perf_counter() - t_batch
                                            )
                                    if page:
                                        t_batch = time.perf_counter()
                                        extras.execute_values(d, insert_sql, page, page_size=cfg.batch_size)
                                        dst.commit(); rows_synced += len(page); batches += 1
                                        self.log.info(
                                            "Final reload batch committed (batches=%d, rows_synced=%d, took %.3fs)",
                                            batches, rows_synced, time.perf_counter() - t_batch
                                        )
                                src.commit()
                    else:
                        self.log.info("Performing incremental fetch where %s > %r", inc_col, dst_max)
                        src_cur = src.cursor(name="pg2pg_stream_upsert")
                        src_cur.itersize = cfg.batch_size

                        order_tail = (", " + ", ".join(_qi(c) for c in pk_cols)) if pk_cols else ""
                        src_sql = (
                            f"SELECT {col_list_sql} FROM {_fq_table(cfg.pg_schema, cfg.pg_table)} "
                            f"WHERE {_qi(inc_col)} >= %s "
                            f"ORDER BY {_qi(inc_col)}{order_tail}"
                        )
                        self.log.info("Source incremental SQL: %s (param=%r)", src_sql, dst_max)
                        src_cur.execute(src_sql, (dst_max,))

                        write_sql = _build_upsert_values_sql(cfg.dst_schema, cfg.dst_table, columns_src, pk_cols) if pk_cols \
                            else _build_insert_values_sql(cfg.dst_schema, cfg.dst_table, columns_src)
                        
                        self.log.info("Write SQL (insert/upsert): %s", write_sql)
                        self.log.info("pk_cols=%s", pk_cols)
                        self.log.info("columns_src=%s", columns_src)
                        self.log.info("write_sql=%s", write_sql)

                        page: List[Tuple] = []
                        with dst.cursor() as d:
                            d.execute("SET LOCAL synchronous_commit TO OFF")
                            for row in src_cur:
                                page.append(tuple(row))
                                if len(page) >= cfg.batch_size:
                                    self.log.debug("Writing batch of %d rows (upsert_time)", len(page))
                                    t_batch = time.perf_counter()
                                    extras.execute_values(d, write_sql, page, page_size=cfg.batch_size)
                                    dst.commit(); rows_synced += len(page); batches += 1; page.clear()
                                    self.log.info("Batch committed (batches=%d, rows_synced=%d, took %.3fs)", batches, rows_synced, time.perf_counter() - t_batch)
                            if page:
                                self.log.debug("Writing final batch of %d rows (upsert_time)", len(page))
                                t_batch = time.perf_counter()
                                extras.execute_values(d, write_sql, page, page_size=cfg.batch_size)
                                dst.commit(); rows_synced += len(page); batches += 1
                                self.log.info("Final batch committed (batches=%d, rows_synced=%d, took %.3fs)", batches, rows_synced, time.perf_counter() - t_batch)
                        src.commit()
                        self.log.debug("Source commit after incremental upsert")

                        # Post-incremental verification and corrective action
                        try:
                            new_src_cnt, _ = _count_and_hash(src, cfg.pg_schema, cfg.pg_table, [], [])
                            new_dst_cnt, _ = _count_and_hash(dst, cfg.dst_schema, cfg.dst_table, [], [])
                            if new_src_cnt != new_dst_cnt:
                                if new_src_cnt > new_dst_cnt:
                                    self.log.warning(
                                        "Post-incremental counts mismatch (src > dst); running upsert-all fallback"
                                    )
                                    src_cur2 = src.cursor(name="pg2pg_resync_all_after_inc")
                                    src_cur2.itersize = cfg.batch_size
                                    src_cur2.execute(f"SELECT {col_list_sql} FROM {_fq_table(cfg.pg_schema, cfg.pg_table)}")

                                    if pk_cols:
                                        write_sql2 = _build_upsert_values_sql(cfg.dst_schema, cfg.dst_table, columns_src, pk_cols)
                                        page2: List[Tuple] = []
                                        with dst.cursor() as d2:
                                            d2.execute("SET LOCAL synchronous_commit TO OFF")
                                            for row in src_cur2:
                                                page2.append(tuple(row))
                                                if len(page2) >= cfg.batch_size:
                                                    t_batch = time.perf_counter()
                                                    extras.execute_values(d2, write_sql2, page2, page_size=cfg.batch_size)
                                                    dst.commit(); rows_synced += len(page2); batches += 1; page2.clear()
                                                    self.log.info(
                                                        "Upsert-all (post-inc) batch committed (batches=%d, rows_synced=%d, took %.3fs)",
                                                        batches, rows_synced, time.perf_counter() - t_batch
                                                    )
                                            if page2:
                                                t_batch = time.perf_counter()
                                                extras.execute_values(d2, write_sql2, page2, page_size=cfg.batch_size)
                                                dst.commit(); rows_synced += len(page2); batches += 1
                                                self.log.info(
                                                    "Final upsert-all (post-inc) batch committed (batches=%d, rows_synced=%d, took %.3fs)",
                                                    batches, rows_synced, time.perf_counter() - t_batch
                                                )
                                        src.commit()
                                    else:
                                        live_fq2 = _fq_table(cfg.dst_schema, cfg.dst_table)
                                        with dst.cursor() as d2:
                                            try:
                                                self.log.info("Attempting TRUNCATE on %s before reload (post-inc) ...", live_fq2)
                                                d2.execute(f"TRUNCATE TABLE {live_fq2} RESTART IDENTITY;")
                                                dst.commit()
                                                self.log.info("TRUNCATE succeeded on %s", live_fq2)
                                            except Exception:
                                                logging.warning("TRUNCATE failed, falling back to DELETE", exc_info=True)
                                                # Not performing DELETE fallback to avoid long-running locks by default

                                        insert_sql2 = _build_simple_insert(cfg.dst_schema, cfg.dst_table, columns_src, pk_cols)
                                        page2: List[Tuple] = []
                                        with dst.cursor() as d2:
                                            d2.execute("SET LOCAL synchronous_commit TO OFF")
                                            for row in src_cur2:
                                                page2.append(tuple(row))
                                                if len(page2) >= cfg.batch_size:
                                                    t_batch = time.perf_counter()
                                                    extras.execute_values(d2, insert_sql2, page2, page_size=cfg.batch_size)
                                                    dst.commit(); rows_synced += len(page2); batches += 1; page2.clear()
                                                    self.log.info(
                                                        "Reload (post-inc) batch committed (batches=%d, rows_synced=%d, took %.3fs)",
                                                        batches, rows_synced, time.perf_counter() - t_batch
                                                    )
                                            if page2:
                                                t_batch = time.perf_counter()
                                                extras.execute_values(d2, insert_sql2, page2, page_size=cfg.batch_size)
                                                dst.commit(); rows_synced += len(page2); batches += 1
                                                self.log.info(
                                                    "Final reload (post-inc) batch committed (batches=%d, rows_synced=%d, took %.3fs)",
                                                    batches, rows_synced, time.perf_counter() - t_batch
                                                )
                                        src.commit()
                                else:
                                    self.log.warning(
                                        "Post-incremental counts mismatch (dst > src). If hard_delete is enabled, it will remove extras later."
                                    )
                        except Exception:
                            self.log.warning("Post-incremental verification failed", exc_info=True)
                except Exception:
                    self.log.error("upsert_time failed; rolling back", exc_info=True)
                    src.rollback(); dst.rollback()
                    raise
            else:
                self.log.error("Unknown sync_method: %s", method)
                raise ValueError(f"Unknown sync_method: {method}")

            # Optional: hard delete (unchanged)
            if cfg.has_hard_delete:
                self.log.info("Hard-delete enabled; comparing counts for potential removals")

                src_cnt = _row_count(src, cfg.pg_schema , cfg.pg_table)
                self.log.info("source schema=%s table=%s count=%d", cfg.pg_schema, cfg.pg_table, src_cnt)

                dst_cnt = _row_count(dst, cfg.dst_schema , cfg.dst_table)
                self.log.info("destination schema=%s table=%s count=%d", cfg.dst_schema, cfg.dst_table, dst_cnt)

                self.log.info("Counts for hard-delete: src=%d dst=%d", src_cnt, dst_cnt)
                if dst_cnt > src_cnt:
                    pk_cols = _pk_cols_list(cfg.primary_key)
                    if pk_cols:
                        import uuid
                        temp_table = f"tmp_src_pks_{cfg.dst_table}_{uuid.uuid4().hex[:8]}"
                        self.log.info("Creating temp table for PKs: %s", temp_table)
                        with dst.cursor() as d:
                            d.execute(f"DROP TABLE IF EXISTS {temp_table};")
                            pk_types = _dst_pk_types(dst, cfg.dst_schema, cfg.dst_table, pk_cols)
                            cols_def = ", ".join(f'{_qi(c)} {t}' for c, t in pk_types)
                            create_tmp_sql = f"CREATE TEMP TABLE {temp_table} ({cols_def}) ON COMMIT DROP;"
                            self.log.debug("Create temp table SQL: %s", create_tmp_sql)
                            d.execute(create_tmp_sql)

                            with src.cursor() as c_src:
                                pk_sel = ', '.join(_qi(col) for col in pk_cols)
                                self.log.debug("Selecting source PKs with: SELECT %s FROM %s", pk_sel, _fq_table(cfg.pg_schema, cfg.pg_table))
                                c_src.execute(f"SELECT {pk_sel} FROM {_fq_table(cfg.pg_schema, cfg.pg_table)}")
                                rows = c_src.fetchall()
                                self.log.info("Fetched %d PK rows from source", len(rows) if rows else 0)
                                if rows:
                                    ins_sql = f"INSERT INTO {temp_table} ({pk_sel}) VALUES %s"
                                    self.log.debug("Bulk insert PKs into temp with execute_values")
                                    extras.execute_values(d, ins_sql, rows, page_size=10_000)

                            idx_sql = f"CREATE INDEX ON {temp_table} ({', '.join(_qi(col) for col in pk_cols)});"
                            self.log.debug("Create index on temp PK table: %s", idx_sql)
                            d.execute(idx_sql)

                            pred = " AND ".join(f"d.{_qi(c)} = t.{_qi(c)}" for c in pk_cols)
                            delete_sql = (
                                f"DELETE FROM {_fq_table(cfg.dst_schema, cfg.dst_table)} d "
                                f"WHERE NOT EXISTS (SELECT 1 FROM {temp_table} t WHERE {pred})"
                            )
                            self.log.debug("Anti-join DELETE SQL: %s", delete_sql)
                            d.execute(delete_sql)
                            removed = d.rowcount
                            dst.commit()
                            self.log.info("Hard deleted %s rows from %s", removed, _fq_table(cfg.dst_schema, cfg.dst_table))
        except Exception:
            self.log.error("Sync failed; rolling back", exc_info=True)
            src.rollback()
            dst.rollback()
            raise
            
        finally:
            try:
                status = dst.get_transaction_status()
                if status != psycopg2.extensions.TRANSACTION_STATUS_IDLE:
                    self.log.warning("Destination connection not idle (status=%s). Rolling back defensively.", status)
                    dst.rollback()
            except Exception:
                self.log.debug("Could not check/rollback destination connection status", exc_info=True)

        result = {
            "mode": method,
            "rows_synced": rows_synced,
            "batches": batches,
            "elapsed": round(time.perf_counter() - t0_sync, 3),
        }
        self.log.info("sync_once result: %s", result)
        return _json_sanitize(result)  # <-- make XCom-safe

    # ------------------------ Lightweight validation ------------------------

    def validate(self, cfg: TableConfig, src, dst) -> Dict[str, Any]:
        self.log.info("validate: method=%s src=%s.%s dst=%s.%s",
                      cfg.sync_method, cfg.pg_schema, cfg.pg_table, cfg.dst_schema, cfg.dst_table)
        t0 = time.perf_counter()
        if cfg.sync_method == "full_replace":
            with src.cursor() as c1, dst.cursor() as c2:
                self.log.info("Validating by row counts")
                c1.execute(f"SELECT COUNT(*) FROM {_fq_table(cfg.pg_schema, cfg.pg_table)}")
                s = int(c1.fetchone()[0])
                c2.execute(f"SELECT COUNT(*) FROM {_fq_table(cfg.dst_schema, cfg.dst_table)}")
                d = int(c2.fetchone()[0])
            ok = (s == d)
            result = {"check": "row_counts", "src": s, "dst": d, "ok": ok, "elapsed": round(time.perf_counter() - t0, 3)}
            self.log.info("Validation (full_replace) result: %s", result)
            return _json_sanitize(result)  # <-- safe

        if cfg.sync_method == "upsert_time":
            inc = cfg.incremental_column
            if not inc:
                result = {"check": "watermark", "error": "no incremental_column", "ok": False}
                self.log.info("Validation failed: %s", result)
                return _json_sanitize(result)  # <-- safe
            src_type = _pg_col_type(src, cfg.pg_schema, cfg.pg_table, inc)
            with src.cursor() as c1, dst.cursor() as c2:
                self.log.info("Validating by watermark on %r (type=%s)", inc, src_type)
                c1.execute(f"SELECT MAX({_qi(inc)}) FROM {_fq_table(cfg.pg_schema, cfg.pg_table)}")
                smax = c1.fetchone()[0] or _default_for_type(src_type)
                c2.execute(f"SELECT MAX({_qi(inc)}) FROM {_fq_table(cfg.dst_schema, cfg.dst_table)}")
                dmax = c2.fetchone()[0] or _default_for_type(src_type)
            ok = (smax == dmax)
            result = {"check": "watermark", "src_max": smax, "dst_max": dmax, "ok": ok, "elapsed": round(time.perf_counter() - t0, 3)}
            self.log.info("Validation (upsert_time) result: %s", result)
            return _json_sanitize(result)  # <-- safe

        result = {"check": "unknown", "ok": False, "elapsed": round(time.perf_counter() - t0, 3)}
        self.log.warning("Validation method unknown: %s", result)
        return _json_sanitize(result)  # <-- safe
