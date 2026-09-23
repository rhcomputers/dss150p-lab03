import pandas as pd

from src.common.audit import utc_now_iso, record_hash

HASH_COLUMNS = [
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
]

OUTPUT_COLUMNS = [
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


def build_curated(staging: dict, run_id: str):
    """Join staging orders/customers/products and create analysis-ready sales rows."""
    orders = staging['orders'].copy()
    customers = staging['customers'].copy()
    products = staging['products'].copy()

    quarantined_product_ids = staging.get('quarantined_product_ids', set())
    quarantined_customer_ids = staging.get('quarantined_customer_ids', set())

    quarantine_parts = [staging.get('quarantine', pd.DataFrame()).copy()]

    def _classify(orders_df, ref_df, fk_col, orphan_reason, upstream_reason):
        """Return (quarantine_rows, ids_to_remove) for one FK."""
        present = set(ref_df[fk_col].astype(str))
        unknown_mask = ~orders_df[fk_col].astype(str).isin(present)

        true_orphan = orders_df[unknown_mask].copy()
        upstream = true_orphan[fk_col].astype(str).isin(
            quarantined_product_ids | quarantined_customer_ids
        )
        q_true = true_orphan[~upstream].copy()
        q_up = true_orphan[upstream].copy()

        q_true['reason'] = orphan_reason
        q_up['reason'] = upstream_reason
        for q in (q_true, q_up):
            q['source'] = 'orders'
            q['pipeline_run_id'] = run_id
            q['quarantined_at_utc'] = utc_now_iso()

        bad_ids = set(true_orphan['order_id'])
        return [q for q in (q_true, q_up) if len(q)], bad_ids

    q_cust, bad_cust = _classify(orders, customers, 'customer_id',
                                 'orphan_customer_reference',
                                 'customer_quarantined_upstream')
    q_prod, bad_prod = _classify(orders, products, 'product_id',
                                 'orphan_product_reference',
                                 'product_quarantined_upstream')

    quarantine_parts.extend(q_cust)
    quarantine_parts.extend(q_prod)

    bad_order_ids = bad_cust | bad_prod
    orders_valid = orders[~orders['order_id'].isin(bad_order_ids)].copy()

    # --- Join ---
    cust_cols = ['customer_id', 'city', 'customer_tier']
    prod_cols = ['product_id', 'name', 'category', 'brand', 'unit_price']

    joined = (
        orders_valid
        .merge(customers[cust_cols].rename(columns={'city': 'customer_city'}),
               on='customer_id', how='inner')
        .merge(products[prod_cols].rename(columns={'name': 'product_name'}),
               on='product_id', how='inner',
               suffixes=('', '_product'))
    )

    # --- Measures ---
    joined['gross_amount'] = (joined['quantity'] * joined['unit_price']).round(2)
    joined['discount_amount'] = (joined['gross_amount'] * joined['discount_pct']).round(2)
    joined['net_amount'] = (joined['gross_amount'] - joined['discount_amount']).round(2)

    # --- Audit columns ---
    joined['source_updated_at'] = joined['updated_at']
    joined['pipeline_run_id'] = run_id
    joined['processed_at_utc'] = utc_now_iso()
    joined['record_hash'] = joined.apply(
        lambda r: record_hash(r.to_dict(), HASH_COLUMNS), axis=1
    )

    curated = joined[OUTPUT_COLUMNS].reset_index(drop=True)

    quarantine = pd.concat(quarantine_parts, ignore_index=True) if any(
        len(p) for p in quarantine_parts
    ) else pd.DataFrame()

    from src.config import path_for
    curated_dir = path_for('curated_dir') / f"run_id={run_id}"
    curated_dir.mkdir(parents=True, exist_ok=True)
    curated.to_parquet(curated_dir / 'sales_order_lines.parquet', index=False)

    quarantine_dir = path_for('quarantine_dir') / f"run_id={run_id}"
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    quarantine.to_parquet(quarantine_dir / 'quarantine_curated.parquet', index=False)

    return {
        'curated': curated,
        'quarantine': quarantine,
    }