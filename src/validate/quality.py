import pandas as pd

from src.config import SETTINGS

ALLOWED_STATUSES = set(SETTINGS['quality']['allowed_order_statuses'])
MIN_QTY = int(SETTINGS['quality']['min_quantity'])
MAX_QTY = int(SETTINGS['quality']['max_quantity'])

REQUIRED_AUDIT_COLUMNS = ['pipeline_run_id', 'processed_at_utc', 'record_hash']


def validate_curated(df) -> list[str]:
    """Return a list of human-readable validation errors.

    Minimum checks: order_id uniqueness/non-null, quantity range,
    nonnegative amounts, allowed statuses, required audit fields.
    """
    errors: list[str] = []

    if df is None or len(df) == 0:
        return ['curated dataset is empty']

    # --- Primary key ---
    if df['order_id'].isna().any():
        errors.append(f"order_id contains {int(df['order_id'].isna().sum())} null(s)")

    dup_mask = df['order_id'].duplicated(keep=False)
    if dup_mask.any():
        dupes = sorted(df.loc[dup_mask, 'order_id'].unique().tolist())
        errors.append(f"order_id is not unique ({len(dupes)} duplicated id(s): {dupes[:5]}...)")

    # --- Foreign keys ---
    for col in ('customer_id', 'product_id'):
        if df[col].isna().any():
            errors.append(f"{col} contains {int(df[col].isna().sum())} null(s)")

    # --- Measures ---
    for col in ('gross_amount', 'discount_amount', 'net_amount'):
        bad = df[col].isna() | (df[col] < 0)
        if bad.any():
            errors.append(f"{col} has {int(bad.sum())} negative or null value(s)")

    # --- Quantity ---
    qty = pd.to_numeric(df['quantity'], errors='coerce')
    bad_qty = qty.isna() | (qty < MIN_QTY) | (qty > MAX_QTY)
    if bad_qty.any():
        errors.append(
            f"quantity out of [{MIN_QTY}, {MAX_QTY}] for {int(bad_qty.sum())} row(s)"
        )

    # --- Discount ---
    disc = pd.to_numeric(df['discount_pct'], errors='coerce')
    bad_disc = disc.isna() | (disc < 0) | (disc > 1)
    if bad_disc.any():
        errors.append(f"discount_pct out of [0, 1] for {int(bad_disc.sum())} row(s)")

    # --- Status ---
    bad_status = ~df['status'].isin(ALLOWED_STATUSES)
    if bad_status.any():
        offending = sorted(df.loc[bad_status, 'status'].unique().tolist())
        errors.append(f"status not in allowed set: {offending}")

    # --- Audit columns ---
    for col in REQUIRED_AUDIT_COLUMNS:
        if col not in df.columns:
            errors.append(f"missing required audit column: {col}")
        elif df[col].isna().any():
            errors.append(f"audit column {col} has {int(df[col].isna().sum())} null(s)")

    return errors