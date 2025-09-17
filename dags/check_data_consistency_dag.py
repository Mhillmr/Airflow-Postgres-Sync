from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Dict, List, Tuple
import json  # <-- added

import pendulum
from dataclasses import asdict

# Airflow
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

# Your modules
from DatabaseComparator import run_db_comparison_callable, summarize_results_callable
from pg_to_pg_sync.alerts import send_discord_alert
from pg_to_pg_factory import _load_catalog
from pg_to_pg_factory import _create_table_config  # (optional) currently unused

log = logging.getLogger(__name__)
logger = log  # keep both names, since your callables use `logger`

# ----------------------------- JSON/XCom helper -----------------------------
def _json_sanitize(val: Any) -> Any:
    """Ensure value is JSON-serializable (safe for Airflow XCom via SDK API)."""
    return json.loads(json.dumps(val, default=str))

# ----------------------------- helpers -----------------------------

def _pair_key(tcfg: Dict[str, Any]) -> Tuple[str, str]:
    """Return (pg_conn_id, dst_pg_conn_id) for grouping."""
    return (tcfg["pg_conn_id"], tcfg["dst_pg_conn_id"])


def _group_tables_by_pair(catalog: Dict[str, Any]) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    """
    Expected catalog structure:
      {
        "tables": [ { ...table config dict... }, ... ]
      }
    Falls back to looking for top-level list if "tables" is absent.
    """
    tables = catalog.get("tables")
    if tables is None and isinstance(catalog, dict):
        # Fallback: try common alternate shapes
        for k in ("items", "configs", "sources", "tables_cfg"):
            if isinstance(catalog.get(k), list):
                tables = catalog[k]
                break
    if not isinstance(tables, list):
        raise AirflowException("Catalog must contain a list of table configs under 'tables' (or a known alias).")

    grouped: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for tcfg in tables:
        if not isinstance(tcfg, dict):
            continue
        key = _pair_key(tcfg)
        grouped[key].append(tcfg)
    return grouped




# ----------------------------- DAG factory -----------------------------

def _build_compare_dag(pg_conn_id: str, dst_pg_conn_id: str):
    """
    Build a DAG that:
      - runs comparison for each table in table_cfgs (dynamic mapping)
      - aggregates results into a single payload
      - summarizes and optionally alerts
    """
    dag_id = f"pg_to_pg_sync_compare__{pg_conn_id}__{dst_pg_conn_id}"

    @dag(
        dag_id=dag_id,
        schedule="0 10 * * *",
        start_date=pendulum.datetime(2025, 8, 18, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        tags=["pg2pg", "database_comparison", pg_conn_id, dst_pg_conn_id],
        description=f"Database comparison for {pg_conn_id} → {dst_pg_conn_id}",
    )
    def _dag():
    
        @task
        def load_pair_table_cfgs() -> List[Dict[str, Any]]:
            """ Load and sanitize table configs for the given pair (pg_conn_id, dst_pg_conn_id). """
            pair_items = _pairs.get((pg_conn_id, dst_pg_conn_id), [])
            logger.info("Loaded %d pair-level items for %s → %s", len(pair_items), pg_conn_id, dst_pg_conn_id)

            clean_tables: List[Dict[str, Any]] = []

            for src in (pair_items or []):
                tables = src.get("tables", [])
                if not isinstance(tables, list):
                    continue

                for tbl in tables:
                    if not isinstance(tbl, dict):
                        continue
                    try:
                        tc_obj = _create_table_config(_catalog, src, tbl)
                        clean_tables.append(asdict(tc_obj))
                    except Exception as e:
                        schema = tbl.get("pg_schema")
                        name   = tbl.get("pg_table")
                        logger.exception("Skip building TableConfig for %s.%s: %s", schema, name, e)

            logger.info("Total sanitized tables for %s → %s: %d",
                        pg_conn_id, dst_pg_conn_id, len(clean_tables))
            log.info("Clean tables: %s", clean_tables)
            # Ensure JSON-safe (in case dataclass has exotic types in future)
            return _json_sanitize(clean_tables)
                
        @task
        def run_db_compare_for_pair(clean_tables: List[Dict[str, Any]]) -> Dict[str, Any]:
            merged: Dict[str, List[str]] = defaultdict(list)

            for tcfg in (clean_tables or []):
                table_name = f"{tcfg.get('pg_schema')}.{tcfg.get('pg_table')}"
                logger.info("▶️ Comparing table %s (%s → %s)", table_name, pg_conn_id, dst_pg_conn_id)

                try:
                    res = run_db_comparison_callable(tcfg)  # {"mismatched_data": {...}, "is_consistent": bool}
                except Exception as e:
                    logger.exception("❌ Error comparing %s: %s", table_name, e)
                    merged[table_name].append(f"ERROR: {e}")
                    continue

                if not isinstance(res, dict):
                    raise AirflowException(f"Comparison callable returned non-dict for {table_name}: {type(res)}")

                inner = res.get("mismatched_data", {}) or {}
                if not isinstance(inner, dict):
                    logger.warning("Unexpected mismatched_data type for %s: %s", table_name, type(inner))
                else:
                    for tbl_key, issues in inner.items():
                        if isinstance(issues, (list, tuple, set)):
                            vals = [str(x) for x in issues if x not in (None, "", False, True)]
                        elif issues not in (None, "", False, True):
                            vals = [str(issues)]
                        else:
                            vals = []
                        if vals:
                            merged[tbl_key].extend(vals)

                status = "mismatch" if not res.get("is_consistent", True) else "consistent"
                logger.info("✅ Done table %s: %s", table_name, status)

            merged = {tbl: sorted(set(vals)) for tbl, vals in merged.items() if vals}

            payload = {
                "mismatched_data": merged,
                "is_consistent": len(merged) == 0,
            }
            logger.info("📦 Pair payload: %s", {"is_consistent": payload["is_consistent"], "count": len(merged)})
            return _json_sanitize(payload)  # <-- XCom-safe

        @task
        def summarize(payload: Dict[str, Any]) -> str:
            # summarize_results_callable *باید* str بده؛ اگر نداد، به str تبدیل می‌کنیم (XCom-safe)
            s = summarize_results_callable(payload, raise_on_inconsistency=False)
            return str(s)

        @task(do_xcom_push=False)  # <-- no XCom push for side-effect-only task
        def alert_if_needed(payload: Dict[str, Any], summary: str) -> None:
            is_consistent = bool(payload.get("is_consistent", True))
            mismatched_map = payload.get("mismatched_data", {}) or {}

            if is_consistent:
                logger.info("🎉 Tables are consistent. No alert.")
                return

            try:
                lines: List[str] = []
                for tbl, issues in sorted(mismatched_map.items()):
                    issues_txt = " | ".join(issues[:10]) + ("" if len(issues) <= 10 else " | …")
                    lines.append(f"- `{tbl}`: {issues_txt}")

                body = "\n".join(lines) if lines else "No details."
                header = f"❗ **Database inconsistency detected**\n`{pg_conn_id}` → `{dst_pg_conn_id}`\n{summary}"
                message = f"{header}\n{body}"

                send_discord_alert(message)
                logger.info("🔔 Alert sent to Discord.")
            except Exception as e:
                logger.exception("Failed to send Discord alert: %s", e)
        
        cfgs    = load_pair_table_cfgs()           
        payload = run_db_compare_for_pair(cfgs)
        summary = summarize(payload)
        alert_if_needed(payload, summary)

    return _dag()


# ----------------------------- DAG registration -----------------------------


_catalog = _load_catalog()
_pairs = _group_tables_by_pair(_catalog)

if not _pairs:
    log.warning("No (pg_conn_id, dst_pg_conn_id) pairs found in catalog.")
else:
    for (pg_conn_id, dst_pg_conn_id), table_cfgs in _pairs.items():
        dag_obj = _build_compare_dag(pg_conn_id, dst_pg_conn_id)
        globals()[dag_obj.dag_id] = dag_obj
