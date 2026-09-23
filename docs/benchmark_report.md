# DSS150P Lab 3 — Storage Format Benchmark Report

## 1. Method

The same logical curated dataset (49,897 rows, produced by python -m src.cli run-all
with PIPELINE_RUN_ID=full_test_1) was materialized in four representations:

- CSV (uncompressed)
- JSON Lines (one object per line)
- Parquet (pyarrow default = Snappy compression)
- PostgreSQL (curated.sales_order_lines)

For each file format, write time was measured once. Full-read and
status='DELIVERED' filtered-read timings were measured 5 times and the
median reported (per §9.2). PostgreSQL was already loaded, so write time is
reported as "already loaded / measured separately"; its size is reported via
pg_total_relation_size('curated.sales_order_lines'), which includes table and
indexes.

All timings are measurements from this machine only and are not universal.

## 2. Machine context

- platform: Windows-10-10.0.19045-SP0
- machine: AMD64
- processor: Intel64 Family 6 Model 165 Stepping 2, GenuineIntel
- python: 3.12.8
- pandas: 2.2.3
- pyarrow: 17.0.0

## 3. Results

| Format | Size (bytes) | Write (s) | Full read median (s) | Filtered read median (s) | Rows |
|---|---:|---:|---:|---:|---:|
| CSV | 14,049,310 | 1.1564 | 0.2793 | 0.2609 | 8,355 |
| JSONL | 29,741,800 | 0.8769 | 0.7405 | 0.7589 | 8,355 |
| Parquet | 5,456,333 | 0.1087 | 0.0581 | 0.0529 | 8,355 |
| PostgreSQL | 15,826,944 | — (already loaded) | 0.5389 | 0.0114 | 8,355 |

## 4. Interpretation

### Q1. Which format was smallest, and what encoding/compression explains it?

Parquet was smallest at ~5.5 MB — 2.6x smaller than CSV and 5.5x smaller than JSON Lines.
Three factors explain this:

- Columnar layout. Values of the same column are stored together, so similar values
  (status, category, customer_city) are adjacent and compress well.
- Dictionary / RLE encoding. Repeated low-cardinality values are stored once and
  referenced by index.
- Snappy compression applied by pyarrow by default.

JSON Lines is the largest because each row repeats all 20 column names. CSV is mid-sized
because column names are stored once, but every value is serialized as text.

### Q2. Which format was fastest for full-dataset read?

Parquet, at ~0.058 s median — ~5x faster than CSV (~0.279 s) and ~13x faster than
JSON Lines (~0.741 s). Reasons:

- No text parsing — types are preserved in the file itself.
- Only the columns requested are decoded.
- Compression means fewer bytes to read from disk.

This does not mean Parquet is best for every workload. Trade-offs:

- Write cost. For appending rows repeatedly, columnar formats are less efficient
  than row-oriented CSV; each write must rewrite (or reorganize) column chunks.
- Streaming / line-by-line consumption. JSON Lines is designed to be parsed one
  record at a time — ideal for streaming into Kafka, log aggregation, or shell tools.
- Human readability / git diffs. CSV and JSONL are text and easy to inspect.
- Query flexibility. Parquet is optimal for column projections; PostgreSQL remains
  better for arbitrary ad-hoc queries and concurrent multi-user access.

### Q3. How did filtered retrieval differ between Parquet and PostgreSQL?

The filter was status = 'DELIVERED' on ~49,897 rows.

- PostgreSQL: 0.0114 s (median) — it scanned the table but did so with a server-side
  query engine that streams only the order_id column back to Python. Network/local
  round-trip is essentially the cost.
- Parquet: 0.0529 s — Parquet has no index on status; pandas read the whole
  Parquet file into a DataFrame and then filtered in memory.

A PostgreSQL index on status would likely widen the gap further:

    CREATE INDEX idx_sol_status ON curated.sales_order_lines (status);

An index allows the planner to satisfy the filter by scanning only matching rows,
eliminating the full-table scan. For high-cardinality filters (e.g., a specific date
range), an index is far more impactful than on a low-cardinality column like status,
where the planner may still choose a sequential scan.

Note also that a similar Parquet-level optimization exists: if the parquet is
partitioned by a column that we filter on, the reader can skip entire partitions —
that's exactly the point of §9.3's year/month partitioning. But status is not a
partition key, so we could not exploit that here.

### Q4. Why is JSON Lines more pipeline-friendly than one giant JSON array?

- Streaming. A JSONL reader can process one record at a time without loading the
  entire file into memory. A JSON array must be fully parsed before you can iterate it.
- Append-friendly. Adding a row to JSONL is a single file append. Adding a row to
  a JSON array requires rewriting the closing bracket and editing inside the array.
- Recovery-friendly. If a JSONL file is truncated mid-write, you lose the last
  record only. If a JSON array is truncated, the file is invalid and unparseable.
- Tool-friendly. Standard Unix tools (grep, awk, head, tail) and most
  log processors natively consume line-oriented text.
- Parallelizable. Files can be split by lines and processed concurrently.

### Q5. What if a partition key has extremely high cardinality or poor query locality?

Two problems:

- High cardinality produces many tiny directories (order_year=2025/order_month=1/...
  is fine, but partitioning by order_id would create ~50,000 single-row files). Small
  files overwhelm metadata and filesystem reads, negating the benefit of partitioning.
- Poor query locality means a query that only filters on a non-partition column
  still needs to scan every partition — worst of both worlds: partition overhead with
  no read savings.

Partition keys should be chosen based on how queries will filter. Year/month works
here because analysts typically query "orders in 2026-01" or "orders in 2026". If
analysts instead queried "all DELIVERED orders regardless of date", partitioning by
year/month buys nothing and might hurt.

## 5. Caveats

- Timings fluctuate with OS, background processes, Docker Desktop on Windows, and disk
  cache state. Medians over 5 runs mitigate but do not eliminate this.
- PostgreSQL's size includes table and index. Parquet's size includes compression.
  Comparing "bytes on disk" between a file format and a server table is inherently
  approximate.
- No formal isolation of CPU frequency, RAM, or disk type was performed. These numbers
  are valid on this machine at this time.