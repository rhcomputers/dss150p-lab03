import pandas as pd

from src.config import SETTINGS, path_for
from src.common.audit import utc_now_iso

ALLOWED_STATUSES = set(SETTINGS['quality']['allowed_order_statuses'])
MIN_QTY = int(SETTINGS['quality']['min_quantity'])
MAX_QTY = int(SETTINGS['quality']['max_quantity'])


def _quarantine_row(reason: str, source: str, run_id: str, payload: dict) -> dict:
    row = dict(payload)
    row['reason'] = reason
    row['source'] = source
    row['pipeline_run_id'] = run_id
    row['quarantined_at_utc'] = utc_now_iso()
    return row


def _dedup_latest(df: pd.DataFrame, key: str, ts_col: str = 'updated_at') -> pd.DataFrame:
    df = df.copy()
    df[ts_col] = pd.to_datetime(df[ts_col], utc=True, errors='coerce')
    df = df.sort_values(ts_col).drop_duplicates(subset=[key], keep='last')
    return df.reset_index(drop=True)


def _stage_customers(raw: pd.DataFrame, run_id: str):
    q = []
    df = _dedup_latest(raw, key='customer_id')

    for col in ('created_at', 'updated_at'):
        df[col] = pd.to_datetime(df[col], utc=True, errors='coerce')

    df['email'] = df['email'].astype('string').str.strip().str.lower()
    df['city'] = df['city'].astype('string').str.strip().str.title()

    df['email_missing'] = df['email'].isna() | (df['email'] == '')

    df['pipeline_run_id'] = run_id
    df['staged_at_utc'] = utc_now_iso()
    return df, pd.DataFrame(q)


def _stage_products(raw: pd.DataFrame, run_id: str):
    q = []
    df = _dedup_latest(raw, key='product_id')

    cat = df['category']
    df['category'] = cat.apply(lambda c: c.get('name') if isinstance(c, dict) else None)
    df['department'] = cat.apply(lambda c: c.get('department') if isinstance(c, dict) else None)

    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')

    bad_price = df['unit_price'].isna() | (df['unit_price'] < 0)
    for _, row in df[bad_price].iterrows():
        q.append(_quarantine_row(
            reason='invalid_or_negative_unit_price',
            source='products',
            run_id=run_id,
            payload=row.to_dict(),
        ))

    df = df[~bad_price].reset_index(drop=True)
    df['pipeline_run_id'] = run_id
    df['staged_at_utc'] = utc_now_iso()
    return df, pd.DataFrame(q)


def _stage_orders(raw: pd.DataFrame, run_id: str):
    q = []
    df = _dedup_latest(raw, key='order_id')

    for col in ('order_timestamp', 'updated_at'):
        df[col] = pd.to_datetime(df[col], utc=True, errors='coerce')

    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['discount_pct'] = pd.to_numeric(df['discount_pct'], errors='coerce')

    bad_ts = df['order_timestamp'].isna() | df['updated_at'].isna()
    bad_qty = df['quantity'].isna() | (df['quantity'] < MIN_QTY) | (df['quantity'] > MAX_QTY)
    bad_status = ~df['status'].isin(ALLOWED_STATUSES)
    bad_price = df['unit_price'].isna() | (df['unit_price'] < 0)
    bad_discount = df['discount_pct'].isna() | (df['discount_pct'] < 0) | (df['discount_pct'] > 1)

    rules = {
        'invalid_timestamp': bad_ts,
        'quantity_out_of_range': bad_qty,
        'status_not_allowed': bad_status,
        'invalid_or_negative_unit_price': bad_price,
        'invalid_discount_pct': bad_discount,
    }

    bad_any = pd.Series(False, index=df.index)
    for reason, mask in rules.items():
        for _, row in df[mask].iterrows():
            q.append(_quarantine_row(
                reason=reason,
                source='orders',
                run_id=run_id,
                payload=row.to_dict(),
            ))
        bad_any = bad_any | mask

    df = df[~bad_any].reset_index(drop=True)
    df['quantity'] = df['quantity'].astype(int)
    df['pipeline_run_id'] = run_id
    df['staged_at_utc'] = utc_now_iso()
    return df, pd.DataFrame(q)


def build_staging(raw_dir, run_id: str):
    """Create cleaned, typed staging datasets."""
    raw_dir = pd.io.common.stringify_path(raw_dir)

    customers_raw = pd.read_csv(f"{raw_dir}/customers.csv")
    products_raw = pd.read_json(f"{raw_dir}/products.json")
    orders_raw = pd.read_csv(f"{raw_dir}/orders.csv")

    customers, q_c = _stage_customers(customers_raw, run_id)
    products, q_p = _stage_products(products_raw, run_id)
    orders, q_o = _stage_orders(orders_raw, run_id)

    quarantine = pd.concat([q_c, q_p, q_o], ignore_index=True) if any(
        len(q) for q in (q_c, q_p, q_o)
    ) else pd.DataFrame()

    staging_dir = path_for('staging_dir') / f"run_id={run_id}"
    quarantine_dir = path_for('quarantine_dir') / f"run_id={run_id}"
    staging_dir.mkdir(parents=True, exist_ok=True)
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    customers.to_parquet(staging_dir / 'customers.parquet', index=False)
    products.to_parquet(staging_dir / 'products.parquet', index=False)
    orders.to_parquet(staging_dir / 'orders.parquet', index=False)
    quarantine.to_parquet(quarantine_dir / 'quarantine.parquet', index=False)

    quarantined_product_ids = set()
    quarantined_customer_ids = set()
    if len(q_p):
        quarantined_product_ids = set(q_p['product_id'].dropna().astype(str))
    if len(q_c):
        quarantined_customer_ids = set(q_c['customer_id'].dropna().astype(str))

    return {
        'customers': customers,
        'products': products,
        'orders': orders,
        'quarantine': quarantine,
        'staging_dir': staging_dir,
        'quarantine_dir': quarantine_dir,
        'quarantined_product_ids': quarantined_product_ids,
        'quarantined_customer_ids': quarantined_customer_ids,
    }