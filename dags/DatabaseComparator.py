import logging
from typing import Dict, List, Optional, Sequence, Tuple, Any
from pg_to_pg_sync.TableConfig import TableConfig

# =====================================================================================
# Import necessary Airflow modules
# =====================================================================================
from airflow.exceptions import AirflowException
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook

import psycopg2
from psycopg2 import sql
import pendulum
from dotenv import load_dotenv

# --- added ---
import json

# ============================== Logging ===============================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ----------------------------- JSON/XCom helper -----------------------------
def _json_sanitize(val: Any) -> Any:
    """Ensure value is JSON-serializable (safe for Airflow XCom via SDK API)."""
    return json.loads(json.dumps(val, default=str))

# ============================== Helper funcs ===============================

_EMPTY_MD5 = "d41d8cd98f00b204e9800998ecf8427e"

def _parse_csv_cols(csv_or_none: Optional[str]) -> List[str]:
    if not csv_or_none:
        return []
    return [c.strip() for c in csv_or_none.split(",") if c.strip()]

def _compose_where(where_sql: Optional[sql.SQL]) -> sql.SQL:
    if where_sql and str(where_sql).strip():
        return sql.SQL(" ").join([sql.SQL("WHERE"), where_sql])
    return sql.SQL("")

# ============================== DatabaseComparator ===============================

class DatabaseComparator:
    """Compare one source table with one destination table according to TableConfig."""

    def __init__(self, cfg: TableConfig):
        self.cfg = cfg
        self.mismatched_data: Dict[str, List[str]] = {}
        self.is_consistent = True

    # ---------- Metadata ----------
    def get_table_columns(self, cursor, schema: str, table: str) -> List[str]:
        logger.info("Fetching columns for %s.%s ...", schema, table)
        q = sql.SQL(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """
        )
        cursor.execute(q, (schema, table))
        cols = [r[0] for r in cursor.fetchall()]
        logger.info("Columns for %s.%s: %s", schema, table, cols)
        return cols
    

    def get_max_value(self, cursor, schema: str, table: str, column: str) -> Optional[int]:
        logger.info("Fetching MAX(%s) for %s.%s ...", column, schema, table)
        q = sql.SQL("SELECT MAX({col}) FROM {sch}.{tbl}").format(
            col=sql.Identifier(column),
            sch=sql.Identifier(schema),
            tbl=sql.Identifier(table),
        )
        try:
            cursor.execute(q)
            row = cursor.fetchone()
            return row[0] if row and row[0] is not None else None
        except psycopg2.Error as e:
            logger.error("Error reading MAX(%s) from %s.%s: %s", column, schema, table, e)
            cursor.connection.rollback()
            return None

    def get_row_count(self, cursor, schema: str, table: str,
                      where_sql: Optional[sql.SQL] = None, params: Tuple = ()) -> int:
        w = _compose_where(where_sql)
        q = sql.SQL("SELECT COUNT(*) FROM {sch}.{tbl} {where_clause}").format(
            sch=sql.Identifier(schema),
            tbl=sql.Identifier(table),
            where_clause=w,
        )
        try:
            cursor.execute(q, params or None)
            row = cursor.fetchone()
            logger.info(
                "Counting %s.%s with WHERE [%s] params=%s",
                schema, table, str(where_sql).strip() if where_sql else "<none>", params
            )
            return int(row[0]) if row else 0
        except psycopg2.Error as e:
            logger.error("Error counting rows in %s.%s: %s", schema, table, e)
            cursor.connection.rollback()
            return -1

    def set_timezone(self, cursor, tz: str = "UTC") -> None:
        try:
            cursor.execute(sql.SQL("SET TIME ZONE {}").format(sql.Literal(tz)))
            logger.info("Session timezone set to %s", tz)
        except psycopg2.Error as e:
            logger.error("Error setting timezone: %s", e)
            cursor.connection.rollback()

    # ---------- Hashing ----------
    def _concat_text_exprs(self, columns: Sequence[str]) -> sql.SQL:
        parts = [
            sql.SQL("COALESCE({}::text, 'NULL')").format(sql.Identifier(c))
            for c in columns
        ]
        sep = sql.SQL(" || '||' || ")
        return sep.join(parts)

    def generate_table_hash(self, cursor, schema: str, table: str,
                            columns: Sequence[str],
                            where_sql: Optional[sql.SQL] = None, params: Tuple = (),
                            order_by_cols: Optional[Sequence[str]] = None) -> Optional[str]:
        concat_cols = self._concat_text_exprs(columns)
        w = _compose_where(where_sql)

        if order_by_cols:
            order_by = sql.SQL(", ").join(sql.Identifier(c) for c in order_by_cols)
            inner = sql.SQL(
                "SELECT md5({concat_cols}) AS row_hash FROM {sch}.{tbl} {where} ORDER BY {ob}"
            ).format(concat_cols=concat_cols, sch=sql.Identifier(schema),
                     tbl=sql.Identifier(table), where=w, ob=order_by)
        else:
            inner = sql.SQL(
                "SELECT md5({concat_cols}) AS row_hash FROM {sch}.{tbl} {where}"
            ).format(concat_cols=concat_cols, sch=sql.Identifier(schema),
                     tbl=sql.Identifier(table), where=w)

        q = sql.SQL(
            "SELECT md5(string_agg(row_hash, '' {order})) FROM ({inner}) t"
        ).format(
            order=sql.SQL("ORDER BY row_hash") if not order_by_cols else sql.SQL(""),
            inner=inner
        )

        try:
            cursor.execute(q, params or None)
            row = cursor.fetchone()
            return row[0] if row and row[0] else _EMPTY_MD5
        except psycopg2.Error as e:
            logger.error("Error hashing %s.%s: %s", schema, table, e)
            cursor.connection.rollback()
            return None

    def generate_column_hash(self, cursor, schema: str, table: str,
                             column_to_hash: str,
                             order_by_cols: Sequence[str],
                             where_sql: Optional[sql.SQL] = None, params: Tuple = ()) -> Optional[str]:
        w = _compose_where(where_sql)
        ob = sql.SQL(", ").join(sql.Identifier(c) for c in order_by_cols)
        q = sql.SQL(
            """
            SELECT md5(string_agg(COALESCE({col}::text, 'NULL'), '' ORDER BY {ob}))
            FROM {sch}.{tbl} {where}
            """
        ).format(
            col=sql.Identifier(column_to_hash),
            ob=ob,
            sch=sql.Identifier(schema),
            tbl=sql.Identifier(table),
            where=w
        )
        try:
            cursor.execute(q, params or None)
            row = cursor.fetchone()
            return row[0] if row and row[0] else _EMPTY_MD5
        except psycopg2.Error as e:
            logger.error("Error hashing column %s.%s.%s: %s", schema, table, column_to_hash, e)
            cursor.connection.rollback()
            return None

    # ---------- Column-by-column compare ----------
    def compare_table_columns(self, main_cursor, backup_cursor,
                              columns: Sequence[str],
                              where_sql: Optional[sql.SQL],
                              params: Tuple,
                              order_by_cols: Sequence[str]) -> List[str]:
        cfg = self.cfg
        table_label = f"{cfg.pg_schema}.{cfg.pg_table} -> {cfg.dst_schema}.{cfg.dst_table}"
        logger.info("Column-level compare for %s", table_label)
        mismatches: List[str] = []

        for col in columns:
            main_hash = self.generate_column_hash(
                main_cursor, cfg.pg_schema, cfg.pg_table, col, order_by_cols, where_sql, params
            )
            bkp_hash = self.generate_column_hash(
                backup_cursor, cfg.dst_schema, cfg.dst_table, col, order_by_cols, where_sql, params
            )
            if main_hash != bkp_hash:
                logger.error("  ❌ Column mismatch: %s", col)
                mismatches.append(col)
            else:
                logger.info("  ✅ Column OK: %s", col)

        return mismatches

    # ---------- Orchestration ----------
    def run_comparison(
        self,
        main_conn,
        backup_conn,
        comparison_date: Optional[str] = None,
        id_offset: int = 0
    ) -> Dict[str, List[str]]:
        """
        Compare source (pg_schema.pg_table) with destination (dst_schema.dst_table).

        Returns: dict { "<src>-><dst>": [issues or mismatched columns...] }
        """
        cfg = self.cfg
        table_key = f"{cfg.pg_schema}.{cfg.pg_table} -> {cfg.dst_schema}.{cfg.dst_table}"
        self.mismatched_data = {table_key: []}

        with main_conn.cursor() as main_cur, backup_conn.cursor() as bkp_cur:
            self.set_timezone(main_cur, "UTC")
            self.set_timezone(bkp_cur, "UTC")

            # 1) Column schemas
            try:
                src_cols = self.get_table_columns(main_cur, cfg.pg_schema, cfg.pg_table)
                dst_cols = self.get_table_columns(bkp_cur, cfg.dst_schema, cfg.dst_table)
            except psycopg2.errors.UndefinedTable:
                logger.error("❌ One of the tables does not exist.")
                self.mismatched_data[table_key] = ["Table missing in one database"]
                self.is_consistent = False
                return _json_sanitize({
                    "mismatched_data": self.mismatched_data,
                    "is_consistent": self.is_consistent,
                })
            if set(src_cols) != set(dst_cols):
                logger.error("❌ Column schema mismatch between source and destination.")
                self.mismatched_data[table_key] = ["Column schema differs between databases"]
                self.is_consistent = False
                return _json_sanitize({
                    "mismatched_data": self.mismatched_data,
                    "is_consistent": self.is_consistent,
                })
            # 2) Where clause based on incremental strategy
            params: Tuple = ()
            where_core: Optional[sql.SQL] = None
            incr = (cfg.incremental_column or "").strip()
            date_cols = {"updated_at", "update_at", "created_at", "used_at"}

            if incr in date_cols and comparison_date:
                logger.info("Using date filter on %s < %s", incr, comparison_date)
                where_core = sql.SQL("{col}::date < %s").format(col=sql.Identifier(incr))
                params = (comparison_date,)
            elif incr == "id" and id_offset > 0:
                max_id = self.get_max_value(main_cur, cfg.pg_schema, cfg.pg_table, "id")
                if max_id is not None:
                    threshold = max_id - id_offset
                    logger.info("Using id filter on id < %s (max_id=%s, offset=%s)", threshold, max_id, id_offset)
                    where_core = sql.SQL("{col} < %s").format(col=sql.Identifier("id"))
                    params = (threshold,)
                else:
                    logger.warning("Could not read MAX(id). Hashing full table.")
            elif incr and incr not in date_cols and incr != "id":
                logger.warning("Incremental column %r not recognized for filtering. Hashing full table.", incr)

            # 3) Choose deterministic ORDER BY columns for hashing
            pk_cols = _parse_csv_cols(cfg.primary_key)
            order_by_cols = pk_cols if pk_cols else ([incr] if incr else [])

            # ensure every chosen order-by column exists
            order_by_cols = [c for c in order_by_cols if c in src_cols]
            if not order_by_cols:
                # fallback: order by all columns to be deterministic (heavier but safe)
                order_by_cols = src_cols

            src_cnt = self.get_row_count(main_cur, cfg.pg_schema, cfg.pg_table, where_core, params)
            dst_cnt = self.get_row_count(bkp_cur, cfg.dst_schema, cfg.dst_table, where_core, params)

            logger.info("Row counts -> source: %s, dest: %s", src_cnt, dst_cnt)

            # 4) Whole-table hash compare
            if src_cnt == dst_cnt:
                logger.info("Hashing source and destination tables ...")
                src_hash = self.generate_table_hash(main_cur, cfg.pg_schema, cfg.pg_table,
                                                    src_cols, where_core, params, order_by_cols)
                dst_hash = self.generate_table_hash(bkp_cur, cfg.dst_schema, cfg.dst_table,
                                                    src_cols, where_core, params, order_by_cols)
                logger.info("Source hash: %s", src_hash)
                logger.info("Dest   hash: %s", dst_hash)

                if src_hash is None or dst_hash is None or src_hash != dst_hash:
                    logger.error("❌ MISMATCH at table level. Checking columns hash ...")

                    mismatched_cols = self.compare_table_columns(
                        main_cur, bkp_cur, src_cols, where_core, params, order_by_cols
                    )
                    self.is_consistent = False
                    self.mismatched_data[table_key] = mismatched_cols or ["Unknown mismatch despite equal counts"]
                else:
                    logger.info("✅ Tables are consistent.")
                    self.is_consistent = True
                    self.mismatched_data[table_key] = []
            else:
                logger.error("❌ Row count mismatch: source=%s, destination=%s", src_cnt, dst_cnt)
                self.mismatched_data[table_key] = [f"Row count mismatch (src={src_cnt}, dst={dst_cnt})"]
                self.is_consistent = False
                return _json_sanitize({
                    "mismatched_data": self.mismatched_data,
                    "is_consistent": self.is_consistent,
                })
        return _json_sanitize({
            "mismatched_data": self.mismatched_data,
            "is_consistent": self.is_consistent,
        })

# =====================================================================================
# Helper: Load config and run DatabaseComparator
# =====================================================================================

def run_db_comparison_callable(tcfg: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Airflow-compatible callable to run the database comparison for ONE table.
    tcfg is a dict version of TableConfig (you already pass this around).
    """
    load_dotenv()

    # Build TableConfig and fetch DB URIs from Airflow Connections
    cfg = TableConfig(**tcfg)
    main_db_uri = BaseHook.get_connection(cfg.pg_conn_id).get_uri()
    backup_db_uri = BaseHook.get_connection(cfg.dst_pg_conn_id).get_uri()

    # Get comparison parameters from Airflow Variables (or defaults)
    comparison_date = pendulum.now("UTC").subtract(days=1).to_date_string()
    id_offset = int(Variable.get("ID_OFFSET", default_var="1000"))

    # Connect and compare
    try:
        main_conn = psycopg2.connect(main_db_uri)
        backup_conn = psycopg2.connect(backup_db_uri)
    except Exception as e:
        raise AirflowException(f"Failed to connect to databases: {e}")

    try:
        comparer = DatabaseComparator(cfg)
        mismatched = comparer.run_comparison(
            main_conn, backup_conn,
            comparison_date=comparison_date,
            id_offset=id_offset,
        )
        return _json_sanitize(mismatched)
    finally:
        main_conn.close()
        backup_conn.close()

def summarize_results_callable(payload: Dict[str, Any], *, raise_on_inconsistency: bool = False) -> str:
    """
    Accepts either:
      A) {"mismatched_data": {table: [details...]}, "is_consistent": bool, ...}
      B) {table: [details...]}
    Returns a one-line summary string and optionally raises if inconsistencies exist.
    """
    # Extract mismatched map if present, otherwise treat payload itself as the map
    payload = _json_sanitize(payload)  # defensively sanitize input
    mismatched = payload.get("mismatched_data", payload)

    if not isinstance(mismatched, dict):
        logger.error("summarize_results_callable received invalid payload: %r", payload)
        raise AirflowException("Invalid mismatched_data payload (expected dict)")

    # Normalize values to lists-of-strings and drop empties/bools
    inconsistent: Dict[str, List[str]] = {}
    for tbl, details in mismatched.items():
        vals: List[str] = []
        if isinstance(details, (list, tuple, set)):
            vals = [str(d) for d in details if d not in (None, "", False, True)]
        else:
            if details not in (None, "", False, True):
                vals = [str(details)]
        if vals:
            inconsistent[str(tbl)] = sorted(set(vals))

    logger.info("\n%s\n📊 CONSISTENCY CHECK SUMMARY\n%s", "=" * 50, "=" * 50)

    if not inconsistent:
        logger.info("🎉 All checked tables are consistent!")
        return "0 table(s) mismatched"

    n = len(inconsistent)
    logger.warning("🚨 Found inconsistencies in %d table(s):", n)
    for table, details in sorted(inconsistent.items()):
        logger.warning("  - Table: '%s'  (%d issue%s)", table, len(details), "" if len(details)==1 else "s")
        for d in details:
            logger.warning("    • %s", d)
    summary = f"{n} table(s) mismatched"
    if raise_on_inconsistency:
        raise AirflowException(f"Database consistency check failed: {summary}")
    return summary
