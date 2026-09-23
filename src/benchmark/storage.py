"""Storage-format benchmark for the DSS150P curated dataset.

Measures the SAME logical row set expressed as CSV, JSON Lines, Parquet, and
PostgreSQL. Reports median-of-N timings and file sizes for the file formats.

All measurements are machine-dependent. The lab explicitly requires reporting
them as measurements from this machine, not as universal claims.
"""
import json
import platform
import statistics
import time
from pathlib import Path

import pandas as pd
import psycopg
from psycopg.rows import dict_row

from src.config import DB, SETTINGS, path_for


REPEATS = int(SETTINGS['storage_benchmark']['repeats'])
FILTER_STATUS = SETTINGS['storage_benchmark']['filter_status']


def _time_call(fn, repeats: int = 1):
    """Run fn() `repeats` times; return (median_seconds, last_result)."""
    times = []
    result = None
    for _ in range(repeats):
        t0 = time.perf_counter()
        result = fn()
        times.append(time.perf_counter() - t0)
    return statistics.median(times), result


def _pg_connect():
    return psycopg.connect(
        host=DB['host'], port=DB['port'], dbname=DB['dbname'],
        user=DB['user'], password=DB['password'], row_factory=dict_row,
    )


def _machine_context() -> dict:
    return {
        'platform': platform.platform(),
        'machine': platform.machine(),
        'processor': platform.processor() or 'unknown',
        'python': platform.python_version(),
        'pandas': pd.__version__,
        'pyarrow': __import__('pyarrow').__version__,
    }


def _materialize_files(df: pd.DataFrame, out_dir: Path) -> dict:
    """Write CSV, JSON Lines, and Parquet; return paths and sizes."""
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / 'curated.csv'
    jsonl_path = out_dir / 'curated.jsonl'
    parquet_path = out_dir / 'curated.parquet'

    csv_write, _ = _time_call(lambda: df.to_csv(csv_path, index=False))
    jsonl_write, _ = _time_call(lambda: df.to_json(
        jsonl_path, orient='records', lines=True, date_format='iso'
    ))
    parquet_write, _ = _time_call(lambda: df.to_parquet(parquet_path, index=False))

    return {
        'csv':     {'path': csv_path,     'size': csv_path.stat().st_size,     'write': csv_write},
        'jsonl':   {'path': jsonl_path,   'size': jsonl_path.stat().st_size,   'write': jsonl_write},
        'parquet': {'path': parquet_path, 'size': parquet_path.stat().st_size, 'write': parquet_write},
    }


def _benchmark_file_format(name: str, path: Path, write_seconds: float,
                           size_bytes: int, repeats: int) -> dict:
    """Measure full and filtered read for one file format."""
    if name == 'csv':
        full_median, _ = _time_call(lambda: pd.read_csv(path), repeats)
        def filtered():
            d = pd.read_csv(path)
            return d[d['status'] == FILTER_STATUS]
        filt_median, last = _time_call(filtered, repeats)
    elif name == 'jsonl':
        full_median, _ = _time_call(
            lambda: pd.read_json(path, orient='records', lines=True), repeats
        )
        def filtered():
            d = pd.read_json(path, orient='records', lines=True)
            return d[d['status'] == FILTER_STATUS]
        filt_median, last = _time_call(filtered, repeats)
    elif name == 'parquet':
        full_median, _ = _time_call(lambda: pd.read_parquet(path), repeats)
        def filtered():
            d = pd.read_parquet(path)
            return d[d['status'] == FILTER_STATUS]
        filt_median, last = _time_call(filtered, repeats)
    else:
        raise ValueError(name)

    return {
        'format': name,
        'size_bytes': size_bytes,
        'write_seconds': round(write_seconds, 4),
        'full_read_median_seconds': round(full_median, 4),
        'filtered_read_median_seconds': round(filt_median, 4),
        'row_count': int(len(last)),
    }


def _benchmark_postgres(df: pd.DataFrame, repeats: int) -> dict:
    """Measure full-table and filtered retrieval from Postgres."""
    # Row count already in curated.sales_order_lines
    with _pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM curated.sales_order_lines;")
            pg_rows = cur.fetchone()['n']

            def full_query():
                cur.execute(
                    "SELECT order_id, customer_id, product_id, order_timestamp, "
                    "customer_city, customer_tier, product_name, category, brand, "
                    "quantity, unit_price, discount_pct, gross_amount, "
                    "discount_amount, net_amount, status, source_updated_at, "
                    "pipeline_run_id, processed_at_utc, record_hash "
                    "FROM curated.sales_order_lines;"
                )
                return cur.fetchall()

            def filtered_query():
                cur.execute(
                    "SELECT order_id FROM curated.sales_order_lines WHERE status = %s;",
                    (FILTER_STATUS,),
                )
                return cur.fetchall()

            full_median, _ = _time_call(full_query, repeats)
            filt_median, last = _time_call(filtered_query, repeats)

            cur.execute("""
                SELECT pg_total_relation_size('curated.sales_order_lines') AS bytes;
            """)
            table_bytes = cur.fetchone()['bytes']

    return {
        'format': 'postgres',
        'size_bytes': int(table_bytes),  # server-side table+index size
        'write_seconds': None,           # measured separately (already loaded)
        'full_read_median_seconds': round(full_median, 4),
        'filtered_read_median_seconds': round(filt_median, 4),
        'row_count': int(len(last)),
    }


def run_benchmark(curated_path, output_dir, repeats: int = 5):
    """Compare the same logical dataset in CSV, JSON Lines, Parquet, and PostgreSQL."""
    curated_path = Path(curated_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not curated_path.exists():
        raise FileNotFoundError(f"Curated Parquet not found: {curated_path}")

    df = pd.read_parquet(curated_path)
    print(f"[benchmark] rows={len(df)} repeats={repeats} filter_status={FILTER_STATUS}")

    # --- File formats ---
    files = _materialize_files(df, output_dir)
    rows = []
    for name in ('csv', 'jsonl', 'parquet'):
        info = files[name]
        print(f"[benchmark] {name}: size={info['size']}B write={info['write']:.3f}s")
        rows.append(_benchmark_file_format(
            name, info['path'], info['write'], info['size'], repeats
        ))

    # --- Postgres ---
    print(f"[benchmark] postgres: measuring full and filtered retrieval")
    rows.append(_benchmark_postgres(df, repeats))

    # --- Write benchmark CSV ---
    df_out = pd.DataFrame(rows)
    out_path = output_dir / 'benchmark_results.csv'
    df_out.to_csv(out_path, index=False)

    # --- Machine context (for the lab's "results are machine-dependent" note) ---
    ctx_path = output_dir / 'benchmark_context.json'
    ctx_path.write_text(json.dumps(_machine_context(), indent=2), encoding='utf-8')

    print(f"[benchmark] wrote {out_path}")
    print(f"[benchmark] wrote {ctx_path}")
    return df_out


def write_partitioned_parquet(df, output_dir):
    """Write Parquet partitioned by order_year/order_month."""
    df = df.copy()
    df['order_year'] = df['order_timestamp'].dt.year
    df['order_month'] = df['order_timestamp'].dt.month
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(
        output_dir,
        partition_cols=['order_year', 'order_month'],
        index=False,
    )
    print(f"[partition] wrote partitioned Parquet under {output_dir}")
    return output_dir