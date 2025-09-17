from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict
import pendulum

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowFailException

from pg_to_pg_sync.connections import pg_conn, dst_pg_conn  # your existing helpers
from pg_to_pg_sync.engine import SyncDataEngine
from pg_to_pg_sync.TableConfig import TableConfig
from DatabaseComparator import run_db_comparison_callable, summarize_results_callable
from pg_to_pg_sync.alerts import send_discord_alert

log = logging.getLogger(__name__)

# ------------------------ Helpers ------------------------
def _json_sanitize(value: Any) -> Any:
    """Ensure value is JSON-serializable (round-trip via dumps/loads)."""
    return json.loads(json.dumps(value, default=str))

# ------------------------ Catalog helpers (DAG-layer) ------------------------
def _load_catalog() -> Dict[str, Any]:
    # log.info("Loading catalog from Airflow Variable 'JSON_CONFIG_PATH'")
    json_config_path = Variable.get("JSON_CONFIG_PATH", default_var="/opt/airflow/dags/catalog.json").strip()
    path = Path(json_config_path)
    if not path.exists():
        raise FileNotFoundError(f"Catalog file not found: {path}")
    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        raise ValueError(f"Catalog file {path} is empty")
    try:
        catalog = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in catalog file {path}: {e}") from e
    # log.info("Loaded catalog from %s", path)
    return catalog

# ------------------------ Configuration helpers ------------------------
def _cfg_get(root: Dict[str, Any], src: Dict[str, Any], tbl: Dict[str, Any], key: str, default=None):
    return tbl.get(key, src.get(key, root.get(key, default)))

def _create_table_config(root: Dict[str, Any], src: Dict[str, Any], tbl: Dict[str, Any]) -> TableConfig:
    pg_conn_id = src["pg_conn_id"]
    dst_pg_conn_id = _cfg_get(root, src, tbl, "dst_pg_conn_id")
    pg_schema = tbl["pg_schema"]
    pg_table = tbl["pg_table"]
    dst_schema = tbl.get("dst_schema", tbl["pg_schema"])
    dst_table = tbl.get("dst_table", tbl["pg_table"])
    sync_method = tbl["sync_method"]
    primary_key = (tbl.get("primary_key") or "").strip() or None
    incremental_column = tbl.get("incremental_column")
    validate = bool(tbl.get("validate", True))
    validate_checksums = bool(tbl.get("validate_checksums", False))
    batch_size = int(_cfg_get(root, src, tbl, "batch_size"))
    optimize_after_sync = bool(_cfg_get(root, src, tbl, "optimize_after_sync", False))
    has_hard_delete = bool(_cfg_get(root, src, tbl, "has_hard_delete"))
    comments = tbl.get("comments", "")

    return TableConfig(
        pg_conn_id=pg_conn_id,
        dst_pg_conn_id=dst_pg_conn_id,
        pg_schema=pg_schema,
        pg_table=pg_table,
        dst_schema=dst_schema,
        dst_table=dst_table,
        sync_method=sync_method,
        primary_key=primary_key,
        incremental_column=incremental_column,
        validate=validate,
        validate_checksums=validate_checksums,
        batch_size=batch_size,
        optimize_after_sync=optimize_after_sync,
        has_hard_delete=has_hard_delete,
        comments=comments,
    )


# ------------------------ DAG creation helpers ------------------------
def _build_single_table_dag(cfg: TableConfig):
    table_name = cfg.pg_table
    dst_conn_id = cfg.dst_pg_conn_id
    dag_id = f"pg_to_pg_sync_{table_name}_{'marketplace' if 'marketplace' in dst_conn_id else 'lend'}"

    # Freeze a JSON-safe config dict at parse time (avoid capturing the object itself)
    def _cfg_to_dict(c: TableConfig) -> Dict[str, Any]:
        try:
            if hasattr(c, "model_dump"):  # pydantic v2
                return c.model_dump()
            from dataclasses import asdict, is_dataclass
            if is_dataclass(c):
                return asdict(c)
        except Exception:
            pass
        d = dict(c.__dict__)
        # (optional) sanitize non-primitive types here if you have any
        return d

    tcfg: Dict[str, Any] = _cfg_to_dict(cfg)

    @dag(
        dag_id=dag_id,
        schedule="*/15 * * * *",
        start_date=pendulum.datetime(2025, 8, 18, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        tags=["pg2pg", cfg.pg_conn_id, cfg.dst_pg_conn_id, f"{cfg.pg_schema}.{cfg.pg_table}"],
        description=f"Sync {cfg.pg_schema}.{cfg.pg_table} → {cfg.dst_schema}.{cfg.dst_table}",
    )
    def sync_dag():

        @task(do_xcom_push=False)
        def ensure_table() -> None:
            log.info("Ensuring table %s.%s", tcfg["pg_schema"], tcfg["pg_table"])
            cfg_obj = TableConfig(**tcfg)
            eng = SyncDataEngine()
            with pg_conn(cfg_obj.pg_conn_id) as src, dst_pg_conn(cfg_obj.dst_pg_conn_id) as dst:
                eng.ensure_table(cfg_obj, src, dst)

        @task(do_xcom_push=False)
        def sync_data() -> None:
            log.info("Syncing data for %s.%s", tcfg["pg_schema"], tcfg["pg_table"])
            cfg_obj = TableConfig(**tcfg)
            eng = SyncDataEngine()
            with pg_conn(cfg_obj.pg_conn_id) as src, dst_pg_conn(cfg_obj.dst_pg_conn_id) as dst:
                eng.sync_once(cfg_obj, src, dst)

        @task(do_xcom_push=False)
        def validate_sync() -> None:
            cfg_obj = TableConfig(**tcfg)
            if not bool(tcfg.get("validate", True)):
                log.info("Validation disabled for %s.%s", tcfg["pg_schema"], tcfg["pg_table"])
                return
            log.info("Validating %s.%s", tcfg["pg_schema"], tcfg["pg_table"])
            eng = SyncDataEngine()
            with pg_conn(cfg_obj.pg_conn_id) as src, dst_pg_conn(cfg_obj.dst_pg_conn_id) as dst:
                eng.validate(cfg_obj, src, dst)

        @task(do_xcom_push=False)
        def alerting() -> None:
            cfg_obj = TableConfig(**tcfg)
            # Only compare/alert for methods that need it
            if cfg_obj.sync_method == "upsert_time":
                log.info("Running DB comparison for %s.%s", tcfg["pg_schema"], tcfg["pg_table"])
                out = run_db_comparison_callable(tcfg)  # should return a dict
                is_consistent = bool(out.get("is_consistent", True))
                if not is_consistent:
                    mismatches = out.get("mismatched_data", [])
                    summary = summarize_results_callable(out) or "No summary provided"
                    schema_src = f"{cfg_obj.pg_schema}.{cfg_obj.pg_table}"
                    schema_dst = f"{cfg_obj.dst_schema}.{cfg_obj.dst_table}"
                    header = f"❗️ **Database inconsistency detected**: `{schema_src}` → `{schema_dst}`"
                    mismatches_txt = ", ".join(map(str, mismatches)) if mismatches else "N/A"
                    message = (
                        f"{header}\n- Summary: {summary}\n"
                        f"- Database:{cfg_obj.pg_conn_id}\n- Mismatches: {mismatches_txt}"
                    )
                    
                    # send_discord_alert(message)
                    log.info("Alert DID NOT SEND due to commenting this part.")
                else:
                    log.info("Comparison OK; no alerting.")
            else:
                log.info("Comparison skipped (sync_method=%s).", cfg_obj.sync_method)

        # linear chain without passing values (no XCom anywhere)
        step1 = ensure_table()
        step2 = sync_data()
        step3 = validate_sync()
        step1 >> step2 >> step3 >> alerting()

    return sync_dag()

# ------------------------ Generate all DAGs from catalog ------------------------
_catalog = _load_catalog()
_created = 0
for _src in _catalog["sources"]:
    for _tbl in _src["tables"]:
        cfg = _create_table_config(_catalog, _src, _tbl)
        dag_obj = _build_single_table_dag(cfg)
        # Ensure Airflow UI shows this file as the DAG source
        try:
            dag_obj.fileloc = __file__
        except Exception:
            pass
        globals()[dag_obj.dag_id] = dag_obj
        _created += 1
