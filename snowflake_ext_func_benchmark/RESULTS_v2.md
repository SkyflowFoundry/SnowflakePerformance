# Snowflake External Function Benchmark — Phase 2 Results

**Date:** 2026-02-14
**Region:** us-east-2 (AWS + Snowflake co-located)

---

## Executive Summary

Phase 1 benchmarks (up to 1M rows) concluded that Snowflake caps external function parallelism at ~16 concurrent calls and warehouse size doesn't matter. **Phase 2 disproved both findings.** At 100M rows, an XL warehouse drove **501 concurrent Lambda invocations** and achieved **~9M rows/sec** — hitting our 500 reserved concurrency limit and causing 52,918 Lambda throttles. Snowflake's parallelism scales with _both_ data volume and warehouse size, and we are now Lambda-constrained, not Snowflake-constrained.

The 50M tokens/sec target appears **architecturally feasible** via external functions if Lambda concurrency is raised and larger warehouses are used.

---

## Phase 2 Results (100M rows)

### Throughput and Concurrency at Scale

| Warehouse | Batch Config | SF Elapsed (ms) | Lambda Calls | Concurrency | Rows/sec |
|-----------|-------------|----------------:|-------------:|------------:|---------:|
| **XS** (1 cr/hr) | default | 47,985 | 24,448 | 64 | 2,083,984 |
| **XS** | 1000 | 122,870 | 122,096 | 64 | 813,868 |
| **XS** | 10000 | 47,500 | 24,448 | 65 | 2,105,263 |
| **XL** (16 cr/hr) | default | 11,658 | 10,919 | **501** | **8,577,800** |
| **XL** | 1000 | 11,168 | 11,411 | **501** | **8,954,154** |
| **XL** | 10000 | 12,180 | 11,406 | **501** | **8,210,180** |

### 1M Row Baseline (unchanged from Phase 1)

| Warehouse | Batch Config | Concurrency | Rows/sec |
|-----------|-------------|------------:|---------:|
| XS | default | 16 | 235,460 |
| XS | 10000 | 16 | 439,367 |
| XL | default | 15 | 212,314 |
| XL | 10000 | 16 | 244,319 |

### CloudWatch Confirmation

| Metric | Value |
|--------|-------|
| Peak concurrent Lambda executions | **500** (at limit) |
| Peak invocations per minute | 20,273 |
| **Total throttles** | **52,918** |
| Throttle rate at peak | ~63% of requests |

---

## Key Findings — What Changed from Phase 1

| # | Phase 1 Finding (1M rows) | Phase 2 Finding (100M rows) | Implication |
|---|---|---|---|
| 1 | Max ~16 concurrent calls | **501 concurrent calls** (capped by Lambda limit) | Parallelism scales massively with data volume |
| 2 | Warehouse size doesn't matter | **XL = 501 concurrent vs XS = 64** (8x) | Warehouse size drives parallelism at scale |
| 3 | Throughput ceiling ~450k rows/sec | **~9M rows/sec** (20x higher) | External functions are far more capable than Phase 1 suggested |
| 4 | 111x gap to 50M target | **~5.6x gap** (and Lambda-constrained) | 50M target is architecturally feasible |
| 5 | Batch size is the biggest lever | Batch size matters less at scale; **concurrency is the lever** | Focus on removing Lambda bottleneck |

### Concurrency Scaling Pattern

| Data Volume | XS Concurrency | XL Concurrency |
|------------|---------------:|---------------:|
| 1M rows | 16 | 15-16 |
| 100M rows | 64 | **501** (capped) |

Snowflake appears to assign concurrent external function calls proportional to `warehouse_nodes × data_partitions`. XL has 16x the compute nodes of XS, explaining the 64 → 501 jump.

---

## Lambda Bottleneck Analysis

XL warehouse at 100M rows **saturated our 500 reserved concurrency limit**:
- 501 unique Lambda instances observed (one over limit due to timing)
- 52,918 throttled requests in 2 minutes
- Actual throughput (~9M rows/sec) was likely **significantly degraded** by throttling
- Snowflake retries throttled calls, adding latency but eventually completing

**Unthrottled throughput projection:** If each of 500 Lambda instances processes batches of ~4,000 rows with ~5ms processing time, theoretical max = `500 × 4000 / 0.005` = **400M rows/sec**. Real-world overhead (network, serialization, Snowflake orchestration) reduces this, but even 10% efficiency = 40M rows/sec.

---

## Proposed Phase 3 Test Plan

### Goal
Remove the Lambda concurrency bottleneck and measure true Snowflake scaling limits across warehouse sizes and data volumes up to 1B rows.

### Changes Required

**1. Raise Lambda reserved concurrency: 500 → 3,000**

This removes the throttling bottleneck. AWS default account limit is 1,000 concurrent executions; a support request may be needed for 3,000.

**2. Revised Test Matrix**

| Dimension | Values | Rationale |
|-----------|--------|-----------|
| **Warehouses** | XS, M, XL, 2XL | XS=baseline, M=mid, XL=confirmed scaling, 2XL=test upper bound |
| **Data volumes** | 10M, 100M, 500M | 10M=transition point, 100M=validated, 500M=scale test |
| **Batch configs** | default, 10000 | Batch size has minimal impact at scale; these two bracket the range |
| **Total combinations** | 4 × 3 × 2 = **24** | |

**Dropped from matrix:**
- 1M rows — too small to trigger real parallelism
- 1B rows — deferred until 500M results confirm scaling trend (generation + benchmark time prohibitive for initial run)
- batch=100, 1000, 2000 — Phase 2 showed batch size barely matters at XL scale; default and 10000 are sufficient
- S, L warehouses — redundant data points between XS and XL

### Estimated Run Time

| Table | Gen Time (2XL) | Benchmark per combo |
|-------|---------------|-------------------|
| 10M | ~5 sec | ~15 sec |
| 100M | ~40 sec | ~60 sec |
| 500M | ~3 min | ~5 min |
| **Total** | ~4 min setup | **~40 min benchmarks** |

### Expected Outcomes

| Warehouse | 100M Concurrency (predicted) | 500M Concurrency (predicted) |
|-----------|---:|---:|
| XS | 64 | 128-256 |
| M | ~125 | 250-500 |
| XL | 1,000-2,000 | 2,000-3,000 |
| 2XL | 2,000-3,000 | 3,000+ |

If 2XL/500M achieves 3,000 concurrent instances with ~4,000 row batches and ~5ms Lambda latency, projected throughput = **~30-50M rows/sec**.

---

## Path to 50M Tokens/sec

| Step | Action | Expected Outcome |
|------|--------|-----------------|
| 1 | Raise Lambda concurrency to 3,000 | Remove throttling bottleneck |
| 2 | Run Phase 3 benchmarks (this plan) | Measure true Snowflake scaling limits |
| 3 | If XL/2XL shows >3,000 concurrent | Raise Lambda to 5,000+ and retest |
| 4 | Optimize Lambda (reduce DynamoDB write latency or make async) | Lower per-call overhead |
| 5 | Replace mock with real Skyflow detokenization | Measure real-world throughput |
| 6 | Multi-query parallelism (if single-query ceiling hit) | Run N concurrent queries partitioned across the table |

### Cost at Target Scale

At 50M rows/sec on XL (16 cr/hr):
- Time to process 1B rows: 20 seconds
- Snowflake cost: 16 credits × (20/3600) hours = **0.089 credits ≈ $0.18**
- Lambda cost (3,000 × 20s × 256MB): **~$0.025**
- Total: **~$0.20 per billion rows**

---

## Appendix: Phase 1 Results Summary (for reference)

Phase 1 tested XS through 2XL warehouses with 10k-1M rows. Key finding at the time was a ~450k rows/sec ceiling. This was misleading because:
1. Data volumes were too small to trigger Snowflake's full parallelism
2. Lambda concurrency (500) wasn't a bottleneck at those scales
3. All warehouses appeared equivalent because all were underutilized

Full Phase 1 data available in `RESULTS.md`.
