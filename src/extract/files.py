from pathlib import Path
import shutil
from src.config import path_for


def extract_sources(run_id: str) -> Path:

    source_dir = path_for('source_dir')
    raw_root = path_for('raw_dir')
    raw_dir = raw_root / f"run_id={run_id}"
    raw_dir.mkdir(parents=True, exist_ok=True)

    source_files = ['customers.csv', 'products.json', 'orders.csv']
    for name in source_files:
        src = source_dir / name
        if not src.exists():
            raise FileNotFoundError(f"Missing source file: {src}")
        dst = raw_dir / name
        shutil.copy2(src, dst)

    return raw_dir
