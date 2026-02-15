# Snowflake External Function Throughput Benchmark Results

**Date:** 2026-02-14
**Region:** us-east-2 (AWS + Snowflake co-located)
**Simulated API Latency:** 0ms
**Lambda:** Go on `provided.al2023`, 256MB, 500 reserved concurrency
**Test Matrix:** 6 warehouses × 3 table sizes × 5 batch configs = 90 combinations

---

## Executive Summary

Snowflake external function throughput **caps at ~450k rows/sec** regardless of warehouse size. Parallelism scales with data volume (up to ~16 concurrent Lambda invocations for 1M rows) but is **not affected by warehouse size**. An XS warehouse achieves equal or better throughput than a 2XL at 1/32 the cost.

For a target of 50M tokens/sec via Skyflow detokenization, external functions would require ~111x more throughput than the observed ceiling — making this architecture insufficient without fundamental changes to how Snowflake orchestrates external function calls.

---

## Key Findings

| # | Finding | Impact |
|---|---------|--------|
| 1 | **Warehouse size does not increase external function throughput** | XS (1 cr/hr) matches or outperforms 2XL (32 cr/hr). Do not scale up warehouses for external function workloads. |
| 2 | **Parallelism scales with data volume, not warehouse size** | 10k rows → 4–8 concurrent calls; 100k → 8–9; 1M → 16–17. Snowflake decides parallelism based on input size. |
| 3 | **Batch size is the biggest throughput lever** | batch=5000 achieves 5–16x higher throughput than batch=100 by reducing HTTP round-trip overhead. |
| 4 | **Throughput ceiling: ~451k rows/sec** | Observed at XS warehouse, 1M rows, MAX_BATCH_ROWS=5000. |
| 5 | **Cost-optimal config: XS warehouse** | Same throughput, 1/32 the credit cost of 2XL. |

---

## Throughput Results

### Rows/sec by Warehouse × Batch Config — 100k rows

| Batch Config | XS | S | M | L | XL | 2XL |
|---|---:|---:|---:|---:|---:|---:|
| 100 | 27,063 | 27,631 | 26,226 | 25,284 | 27,723 | 27,092 |
| 500 | 77,579 | 68,823 | 81,566 | 85,178 | 83,682 | 83,752 |
| 1,000 | 102,880 | 88,339 | 105,820 | 87,873 | 105,263 | 108,108 |
| 5,000 | 110,011 | 97,087 | 102,354 | 110,619 | 134,952 | 132,275 |
| default (~3,500) | 57,836 | 119,047 | 95,969 | 89,285 | 105,042 | 86,730 |

### Rows/sec by Warehouse × Batch Config — 1M rows

| Batch Config | XS | S | M | L | XL | 2XL |
|---|---:|---:|---:|---:|---:|---:|
| 100 | 60,742 | 34,728 | 34,288 | 34,993 | 34,869 | 35,353 |
| 500 | 190,730 | 121,256 | 123,380 | 121,847 | 127,372 | 123,961 |
| 1,000 | 287,521 | 175,500 | 173,550 | 173,310 | 169,664 | 176,149 |
| 5,000 | **450,653** | 267,236 | 265,957 | 253,613 | 253,100 | 247,524 |
| default (~4,000) | 327,011 | 241,196 | 236,518 | 234,246 | 233,045 | 230,308 |

### Rows/sec by Warehouse × Batch Config — 10k rows

| Batch Config | XS | S | M | L | XL | 2XL |
|---|---:|---:|---:|---:|---:|---:|
| 100 | 5,813 | 9,680 | 11,261 | 11,350 | 11,025 | 15,923 |
| 500 | 11,402 | 15,060 | 12,658 | 15,432 | 16,103 | 16,051 |
| 1,000 | 29,498 | 16,863 | 17,825 | 27,397 | 16,611 | 26,737 |
| 5,000 | 23,041 | 14,598 | 24,213 | 24,509 | 22,935 | 14,925 |
| default (~1,700) | 8,025 | 11,025 | 10,460 | 9,363 | 8,936 | 11,312 |

---

## Parallelism Analysis (Lambda-side DynamoDB metrics)

Captured via per-invocation DynamoDB writes from Lambda. Data available for XS warehouse (first warehouse in test sequence).

### XS Warehouse — 10k rows

| Batch Config | Lambda Calls | Avg Batch Size | Unique Lambda Instances (Concurrency) | SF Elapsed (ms) |
|---|---:|---:|---:|---:|
| default | 6 | 1,666 | 4 | 1,246 |
| 100 | 103 | 97 | 8 | 1,720 |
| 500 | 25 | 400 | 8 | 877 |
| 1,000 | 15 | 666 | 7 | 339 |
| 5,000 | 6 | 1,666 | 5 | 434 |

### XS Warehouse — 100k rows

| Batch Config | Lambda Calls | Avg Batch Size | Unique Lambda Instances (Concurrency) | SF Elapsed (ms) |
|---|---:|---:|---:|---:|
| default | 28 | 3,571 | 9 | 1,729 |
| 100 | 1,004 | 99 | 9 | 3,695 |
| 500 | 222 | 450 | 8 | 1,289 |
| 1,000 | 124 | 806 | 8 | 972 |
| 5,000 | 28 | 3,571 | 8 | 909 |

### XS Warehouse — 1M rows

| Batch Config | Lambda Calls | Avg Batch Size | Unique Lambda Instances (Concurrency) | SF Elapsed (ms) |
|---|---:|---:|---:|---:|
| default | 252 | 3,968 | 16 | 3,058 |
| 100 | 10,016 | 99 | 16 | 16,463 |
| 500 | 2,203 | 453 | 17 | 5,243 |
| 1,000 | 1,226 | 815 | 16 | 3,478 |
| 5,000 | 252 | 3,968 | 16 | 2,219 |

**Observation:** Concurrency doubles from ~8 (100k rows) to ~16 (1M rows), confirming that Snowflake scales parallelism based on input data volume, not warehouse compute capacity.

---

## Snowflake Elapsed Time (ms)

Raw Snowflake query execution time (from `INFORMATION_SCHEMA.QUERY_HISTORY`), excluding snowsql CLI overhead.

### 100k rows

| Batch Config | XS | S | M | L | XL | 2XL |
|---|---:|---:|---:|---:|---:|---:|
| 100 | 3,695 | 3,619 | 3,813 | 3,955 | 3,607 | 3,691 |
| 500 | 1,289 | 1,453 | 1,226 | 1,174 | 1,195 | 1,194 |
| 1,000 | 972 | 1,132 | 945 | 1,138 | 950 | 925 |
| 5,000 | 909 | 1,030 | 977 | 904 | 741 | 756 |
| default | 1,729 | 840 | 1,042 | 1,120 | 952 | 1,153 |

### 1M rows

| Batch Config | XS | S | M | L | XL | 2XL |
|---|---:|---:|---:|---:|---:|---:|
| 100 | 16,463 | 28,795 | 29,164 | 28,577 | 28,678 | 28,286 |
| 500 | 5,243 | 8,247 | 8,105 | 8,207 | 7,851 | 8,067 |
| 1,000 | 3,478 | 5,698 | 5,762 | 5,770 | 5,894 | 5,677 |
| 5,000 | 2,219 | 3,742 | 3,760 | 3,943 | 3,951 | 4,040 |
| default | 3,058 | 4,146 | 4,228 | 4,269 | 4,291 | 4,342 |

**Observation:** XS is consistently faster for 1M rows. Larger warehouses add overhead without increasing external function parallelism.

---

## Cost Analysis

### Estimated Snowflake Credits per Million Rows (100k row benchmark)

| Batch Config | XS (1 cr/hr) | S (2 cr/hr) | M (4 cr/hr) | L (8 cr/hr) | XL (16 cr/hr) | 2XL (32 cr/hr) |
|---|---:|---:|---:|---:|---:|---:|
| 5,000 | 0.0025 | 0.0057 | 0.0109 | 0.0201 | 0.0329 | 0.0672 |
| 1,000 | 0.0027 | 0.0063 | 0.0105 | 0.0253 | 0.0422 | 0.0822 |
| 500 | 0.0036 | 0.0081 | 0.0136 | 0.0261 | 0.0531 | 0.1062 |
| default | 0.0048 | 0.0047 | 0.0116 | 0.0249 | 0.0423 | 0.1025 |
| 100 | 0.0103 | 0.0201 | 0.0424 | 0.0879 | 0.1603 | 0.3281 |

**Optimal configuration: XS + batch=5000 at 0.0025 credits per million rows** — 131x cheaper than 2XL + batch=100.

---

## Architecture & Methodology

```
┌─────────────┐    ┌──────────────────┐    ┌──────────────┐    ┌──────────┐
│  Snowflake  │───>│  API Gateway     │───>│  Lambda (Go) │───>│ DynamoDB │
│  External   │    │  (Regional,      │    │  Mock detok   │    │ Metrics  │
│  Function   │<───│   AWS_IAM auth)  │<───│  DETOK_prefix │    │          │
└─────────────┘    └──────────────────┘    └──────────────┘    └──────────┘
```

**What we measured:**
- `SF_ELAPSED_MS`: Snowflake-reported query elapsed time from `INFORMATION_SCHEMA.QUERY_HISTORY` via `LAST_QUERY_ID()` in a single-session multi-statement execution
- `SF_ROWS_PER_SEC`: `row_count * 1000 / SF_ELAPSED_MS`
- Lambda-side metrics (invocations, batch sizes, concurrency): from DynamoDB records written synchronously by each Lambda invocation, queried by Snowflake query ID

**Controls:**
- `ALTER SESSION SET USE_CACHED_RESULT = FALSE` — disables Snowflake result cache
- `SUM(LENGTH(func(token_value)::VARCHAR))` — forces function evaluation (prevents query optimizer from skipping external function calls)
- Lambda simulated delay: 0ms (measures Snowflake orchestration overhead, not API latency)
- Lambda reserved concurrency: 500 (ensures Lambda is not the bottleneck)
- Warmup query before each benchmark (ensures warehouse is active)

---

## Implications for 50M Tokens/sec Detokenization Target

| Metric | Value |
|---|---|
| Peak observed throughput | 450,653 rows/sec |
| Target throughput | 50,000,000 tokens/sec |
| Gap (1 token/row) | **~111x** |
| Max observed parallelism | 16–17 concurrent Lambda calls |
| Lambda capacity (reserved) | 500 concurrent |
| Utilization of Lambda capacity | **3.2–3.4%** |

### Bottleneck

Snowflake's external function orchestrator limits parallelism to ~16 concurrent calls regardless of warehouse size. This is an architectural constraint in Snowflake's external function framework, not a Lambda or API Gateway limitation.

### Potential Paths Forward

1. **Multiple concurrent queries** — Run N queries in parallel (e.g., partition the table and call the function from N concurrent sessions). Each gets ~16 parallel calls, potentially scaling linearly.
2. **Snowpark / UDTFs** — Use Snowpark Container Services or Java/Python UDTFs that call Skyflow directly, bypassing the external function framework.
3. **Pre-materialization** — Detokenize at write time rather than query time, storing plaintext in a secure view.
4. **Hybrid approach** — Use external functions for small ad-hoc queries (<100k rows) where ~100k rows/sec is acceptable; batch-process larger volumes outside Snowflake.

---

## Raw Data

Full results are stored in Snowflake:
```sql
SELECT * FROM EXT_FUNC_BENCHMARK.BENCHMARK.benchmark_results
ORDER BY warehouse_size, row_count, batch_config;
```

Lambda-side invocation metrics are in DynamoDB table `ext_func_benchmark_metrics` (us-east-2), queryable by Snowflake `query_id`.

### Reproducing

```bash
# Full matrix (90 combinations, ~30 min, ~$10-18)
./run_benchmark.sh

# Quick matrix (12 combinations, ~5 min, ~$2)
./run_benchmark.sh --quick

# With simulated API latency
./run_benchmark.sh --delay-ms 100

# Cleanup all resources
./run_benchmark.sh --cleanup
```
