from dataclasses import dataclass

# ============================== Config model ===============================

@dataclass(frozen=True)
class TableConfig:
    pg_conn_id: str
    dst_pg_conn_id: str
    pg_schema: str
    pg_table: str
    dst_schema: str
    dst_table: str
    sync_method: str                   # "full_replace" | "upsert_time"
    primary_key: str | None = None     # comma-separated string
    incremental_column: str | None = None
    batch_size: int = 20_000
    validate: bool = True
    validate_checksums: bool = False
    optimize_after_sync: bool = False
    has_hard_delete: bool = False
    comments: str = ""

