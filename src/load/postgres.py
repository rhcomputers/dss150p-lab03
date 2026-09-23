import psycopg
from psycopg.rows import dict_row

from src.config import DB
from src.common.audit import utc_now_iso


INSERT_COLUMNS = [
    'order_id',
    'customer_id',
    'product_id',
    'order_timestamp',
    'customer_city',
    'customer_tier',
    'product_name',
    'category',
    'brand',
    'quantity',
    'unit_price',
    'discount_pct',
    'gross_amount',
    'discount_amount',
    'net_amount',
    'status',
    'source_updated_at',
    'pipeline_run_id',
    'processed_at_utc',
    'record_hash',
]

UPSERT_SQL = f"""
INSERT INTO curated.sales_order_lines ({', '.join(INSERT_COLUMNS)})
VALUES ({', '.join(['%s'] * len(INSERT_COLUMNS))})
ON CONFLICT (order_id) DO UPDATE
SET
    customer_id      = EXCLUDED.customer_id,
    product_id       = EXCLUDED.product_id,
    order_timestamp  = EXCLUDED.order_timestamp,
    customer_city    = EXCLUDED.customer_city,
    customer_tier    = EXCLUDED.customer_tier,
    product_name     = EXCLUDED.product_name,
    category         = EXCLUDED.category,
    brand            = EXCLUDED.brand,
    quantity         = EXCLUDED.quantity,
    unit_price       = EXCLUDED.unit_price,
    discount_pct     = EXCLUDED.discount_pct,
    gross_amount     = EXCLUDED.gross_amount,
    discount_amount  = EXCLUDED.discount_amount,
    net_amount       = EXCLUDED.net_amount,
    status           = EXCLUDED.status,
    source_updated_at = EXCLUDED.source_updated_at,
    pipeline_run_id  = EXCLUDED.pipeline_run_id,
    processed_at_utc = EXCLUDED.processed_at_utc,
    record_hash      = EXCLUDED.record_hash
WHERE curated.sales_order_lines.record_hash
      IS DISTINCT FROM EXCLUDED.record_hash;
"""


def _connect():
    return psycopg.connect(
        host=DB['host'],
        port=DB['port'],
        dbname=DB['dbname'],
        user=DB['user'],
        password=DB['password'],
        row_factory=dict_row,
    )


def _to_row(df_row):
    """Convert a pandas row into the tuple expected by UPSERT_SQL."""
    out = []
    for col in INSERT_COLUMNS:
        v = df_row.get(col)
        # psycopg handles pandas Timestamp if we hand it a Python datetime.
        if hasattr(v, 'to_pydatetime'):
            v = v.to_pydatetime()
        out.append(v)
    return tuple(out)


def upsert_curated(df, run_id: str) -> int:
    """Load curated.sales_order_lines using rerun-safe UPSERT semantics.

    Returns the number of rows actually written (inserted or updated).
    Reruns with unchanged business content should write zero rows.
    """
    if df is None or len(df) == 0:
        return 0

    rows = [_to_row(r) for r in df.to_dict(orient='records')]

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.executemany(UPSERT_SQL, rows)
            affected = cur.rowcount  # rows inserted or updated
        conn.commit()

    return affected


def start_run(run_id: str) -> None:
    """Insert a started row into audit.pipeline_runs (idempotent)."""
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audit.pipeline_runs (pipeline_run_id, started_at_utc, status)
                VALUES (%s, %s, 'RUNNING')
                ON CONFLICT (pipeline_run_id) DO UPDATE
                SET started_at_utc = EXCLUDED.started_at_utc,
                    status = 'RUNNING',
                    message = NULL
                """,
                (run_id, utc_now_iso()),
            )
        conn.commit()


def finish_run(run_id: str, status: str, rows_staging: int | None = None,
               rows_curated: int | None = None, rows_quarantined: int | None = None,
               message: str | None = None) -> None:
    """Mark the run as completed/failed in audit.pipeline_runs."""
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE audit.pipeline_runs
                SET completed_at_utc = %s,
                    status = %s,
                    rows_staging = %s,
                    rows_curated = %s,
                    rows_quarantined = %s,
                    message = %s
                WHERE pipeline_run_id = %s
                """,
                (utc_now_iso(), status, rows_staging, rows_curated,
                 rows_quarantined, message, run_id),
            )
        conn.commit()

def load_partition(df, year: int, month: int, run_id: str) -> int:
    """Load only a selected year/month partition and record audit.partition_loads.

    Reuses the same rerun-safe UPSERT as upsert_curated, so re-loading the
    same partition writes zero rows if nothing changed.
    Returns the number of rows actually written (insert+update).
    """
    if df is None or len(df) == 0:
        rows_written = 0
    else:
        rows = [_to_row(r) for r in df.to_dict(orient='records')]
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(UPSERT_SQL, rows)
                rows_written = cur.rowcount
            conn.commit()

    partition_key = f"{year:04d}-{month:02d}"
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audit.partition_loads
                    (partition_key, loaded_at_utc, row_count, pipeline_run_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (partition_key) DO UPDATE
                SET loaded_at_utc = EXCLUDED.loaded_at_utc,
                    row_count     = EXCLUDED.row_count,
                    pipeline_run_id = EXCLUDED.pipeline_run_id
                """,
                (partition_key, utc_now_iso(), len(df) if df is not None else 0, run_id),
            )
        conn.commit()

    return rows_written