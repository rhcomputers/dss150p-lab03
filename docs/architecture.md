# DSS150P Lab 3 — Pipeline Architecture

This document records the module evaluation required by Lab Activity #3 §7.2
Task B. Each Python package under `src/` has exactly one primary responsibility,
and it must not contain the concerns listed in the "Must NOT contain" column.

## Module responsibility map

| Module | Primary responsibility | Must NOT contain |
|---|---|---|
| `src/extract/` | Acquire/copy source snapshots into the raw layer. | Business calculations. |
| `src/transform/` | Staging cleanup and curated business rules. | Airflow-specific code. |
| `src/load/` | PostgreSQL persistence and rerun-safe UPSERT. | Source-specific cleaning. |
| `src/validate/` | Data and contract assertions. | Transformation side effects. |
| `src/benchmark/` | Format materialization and timing measurement. | Production business logic. |
| `src/cli.py` | Thin composition and entry points. | Duplicate transformation implementations. |
| `src/config.py` | Translate `config/settings.yml` + `.env` into a single settings object. | Command orchestration. |
| `src/common/audit.py` | Deterministic audit helpers (UTC now, run id, record hash). | Transformation rules. |

## Layer semantics

The pipeline emits four data layers under `data/`:

| Layer | Purpose | Examples |
|---|---|---|
| `data/raw/` | Reproducible source snapshot, keyed by `run_id=<id>/`. | Untouched copies of `customers.csv`, `products.json`, `orders.csv`. |
| `data/staging/` | Typing, normalization, source-level dedup, validity checks. | Normalized emails/cities, flattened product category, parsed timestamps. |
| `data/curated/` | Cross-source joins and consumer-ready shape. | Sales order lines with monetary measures and audit columns. |
| `data/quarantine/` | Invalid records kept with the reason for rejection. | Bad quantity, negative price, orphan customer/product. |

## Composition rules

- `src/cli.py` is a **thin composition layer**. It parses arguments, instantiates
  `run_id`, calls the correct module function, and prints a short summary. It
  must not re-implement any transformation that lives in `src/transform/`.
- Airflow (§10) invokes `python -m src.cli <command>` and never embeds business
  logic in the DAG file.
- Configuration is centralized in `src/config.py`. No password, hostname,
  filesystem root, or environment-specific value appears in business logic.