import argparse
import json

from src.config import PROJECT_ROOT, DB, SETTINGS
from src.common.audit import new_run_id
from src.extract.files import extract_sources
from src.transform.staging import build_staging
from src.transform.curated import build_curated
from src.validate.quality import validate_curated
from src.load.postgres import upsert_curated, start_run, finish_run


def _summarize_staging(staging: dict) -> None:
    print(f"  customers rows: {len(staging['customers'])}")
    print(f"  products rows:  {len(staging['products'])}")
    print(f"  orders rows:    {len(staging['orders'])}")
    print(f"  quarantine rows: {len(staging['quarantine'])}")


def _run_full(run_id: str) -> None:
    start_run(run_id)
    try:
        print(f"[extract] run_id={run_id}")
        raw_dir = extract_sources(run_id)
        print(f"  raw snapshot at: {raw_dir}")

        print(f"[transform/staging] run_id={run_id}")
        staging = build_staging(raw_dir, run_id)
        _summarize_staging(staging)

        print(f"[transform/curated] run_id={run_id}")
        curated = build_curated(staging, run_id)
        print(f"  curated rows:    {len(curated['curated'])}")
        print(f"  quarantine rows: {len(curated['quarantine'])}")

        errors = validate_curated(curated['curated'])
        if errors:
            print(f"[validate] {len(errors)} error(s) found:")
            for e in errors:
                print(f"  - {e}")
            raise RuntimeError(f"curated dataset failed validation with {len(errors)} error(s)")
        print("[validate] OK")

        print(f"[load] upserting curated rows")
        written = upsert_curated(curated['curated'], run_id)
        print(f"  rows written (insert+update): {written}")

        finish_run(
            run_id,
            'SUCCESS',
            rows_staging=len(staging['orders']),
            rows_curated=len(curated['curated']),
            rows_quarantined=len(curated['quarantine']),
        )
    except Exception as e:
        finish_run(run_id, 'FAILED', message=str(e)[:500])
        raise


def main():
    parser = argparse.ArgumentParser(description='DSS150P modular pipeline')
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('validate-env')
    sub.add_parser('extract')
    sub.add_parser('transform')
    sub.add_parser('load')
    sub.add_parser('validate')
    sub.add_parser('begin-run')
    sub.add_parser('end-run')
    b = sub.add_parser('benchmark'); b.add_argument('--repeats', type=int, default=5)
    p = sub.add_parser('load-partition'); p.add_argument('--year', type=int, required=True); p.add_argument('--month', type=int, required=True)
    sub.add_parser('run-all')
    args = parser.parse_args()

    run_id = new_run_id()

    if args.command == 'validate-env':
        print('PROJECT_ROOT=', PROJECT_ROOT)
        print('DB host/database=', DB['host'], DB['dbname'])
        print('Configured source=', SETTINGS['pipeline']['source_dir'])
        return

    if args.command == 'run-all':
        _run_full(run_id)
        return

    if args.command == 'extract':
        raw_dir = extract_sources(run_id)
        print(f"[extract] run_id={run_id} -> {raw_dir}")
        return

    if args.command == 'transform':
        from src.config import path_for
        raw_dir = path_for('raw_dir') / f"run_id={run_id}"
        if not raw_dir.exists():
            raise FileNotFoundError(
                f"No raw snapshot at {raw_dir}. Run `extract` with the same "
                f"PIPELINE_RUN_ID first, or use `run-all`."
            )
        staging = build_staging(raw_dir, run_id)
        _summarize_staging(staging)
        curated = build_curated(staging, run_id)
        print(f"  curated rows:    {len(curated['curated'])}")
        print(f"  quarantine rows: {len(curated['quarantine'])}")
        return

    if args.command == 'load':
        from src.config import path_for
        curated_path = path_for('curated_dir') / f"run_id={run_id}" / "sales_order_lines.parquet"
        if not curated_path.exists():
            raise FileNotFoundError(
                f"No curated Parquet at {curated_path}. Run `transform` with the "
                f"same PIPELINE_RUN_ID first, or use `run-all`."
            )
        import pandas as pd
        df = pd.read_parquet(curated_path)
        written = upsert_curated(df, run_id)
        print(f"[load] run_id={run_id} rows written (insert+update): {written}")
        return

    if args.command == 'validate':
        from src.config import path_for
        curated_path = path_for('curated_dir') / f"run_id={run_id}" / "sales_order_lines.parquet"
        if not curated_path.exists():
            raise FileNotFoundError(
                f"No curated Parquet at {curated_path}. Run `transform` first."
            )
        import pandas as pd
        df = pd.read_parquet(curated_path)
        errors = validate_curated(df)
        if errors:
            print(f"[validate] FAILED with {len(errors)} error(s):")
            for e in errors:
                print(f"  - {e}")
            raise SystemExit(1)
        print(f"[validate] OK ({len(df)} rows)")
        return

    if args.command == 'begin-run':
        start_run(run_id)
        print(f"[begin-run] run_id={run_id} recorded in audit.pipeline_runs")
        return

    if args.command == 'end-run':
        from src.config import path_for
        import pandas as pd
        staging_dir = path_for('staging_dir') / f"run_id={run_id}"
        curated_dir = path_for('curated_dir') / f"run_id={run_id}"
        quarantine_dir = path_for('quarantine_dir') / f"run_id={run_id}"
        orders_path = staging_dir / 'orders.parquet'
        curated_path = curated_dir / 'sales_order_lines.parquet'
        # The merged quarantine file (staging + curated) is written by build_curated.
        quarantine_path = quarantine_dir / 'quarantine_curated.parquet'
        rows_staging = len(pd.read_parquet(orders_path)) if orders_path.exists() else None
        rows_curated = len(pd.read_parquet(curated_path)) if curated_path.exists() else None
        rows_quarantined = len(pd.read_parquet(quarantine_path)) if quarantine_path.exists() else None
        finish_run(run_id, 'SUCCESS',
                   rows_staging=rows_staging,
                   rows_curated=rows_curated,
                   rows_quarantined=rows_quarantined)
        print(f"[end-run] run_id={run_id} marked SUCCESS (staging={rows_staging}, curated={rows_curated}, quarantined={rows_quarantined})")
        return

    if args.command == 'benchmark':
        from src.benchmark.storage import run_benchmark
        from src.config import path_for
        curated_path = path_for('curated_dir') / f"run_id={run_id}" / "sales_order_lines.parquet"
        output_dir = path_for('benchmark_dir')
        run_benchmark(curated_path, output_dir, repeats=args.repeats)
        return

    if args.command == 'load-partition':
        from src.load.postgres import load_partition
        from src.config import path_for
        import pandas as pd
        part_path = path_for('partition_dir') / f"order_year={args.year}" / f"order_month={args.month}"
        if not part_path.exists():
            raise FileNotFoundError(f"No partition at {part_path}. Run `benchmark` first to build partitioned Parquet.")
        df = pd.read_parquet(part_path)
        written = load_partition(df, args.year, args.month, run_id)
        print(f"[load-partition] {args.year}-{args.month:02d} rows written: {written}")
        return

    raise NotImplementedError(f'Unhandled command: {args.command}')


if __name__ == '__main__':
    main()