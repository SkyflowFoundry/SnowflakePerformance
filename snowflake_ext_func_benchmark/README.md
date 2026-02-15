# Snowflake External Function Throughput Benchmark

Measures how Snowflake batches and parallelizes external function calls to determine detokenization throughput ceilings.

## What This Measures

- **Warehouse scaling**: Does bigger warehouse = more external function parallelism?
- **Batch sizes**: What batch sizes does Snowflake actually send to the Lambda?
- **MAX_BATCH_ROWS effect**: Does the external function setting change throughput?
- **Raw throughput ceiling**: Max rows/sec before adding real API latency
- **Cost efficiency**: Credits per million rows at each warehouse size

## Prerequisites

- **AWS CLI** configured with credentials (`aws sts get-caller-identity`)
- **Snowflake CLI** (`snow`) configured with a connection (`snow sql -q "SELECT 1"`)
- **Go** 1.21+ (`go version`)
- **jq** (`brew install jq`)

## Quick Start

```bash
# Full benchmark (all warehouse sizes, all table sizes, all batch configs)
./run_benchmark.sh

# Quick mode (reduced matrix — ~15 min instead of ~60 min)
./run_benchmark.sh --quick

# With simulated API latency
./run_benchmark.sh --skip-deploy --skip-setup --delay-ms 100

# Cleanup everything
./run_benchmark.sh --cleanup
```

## CLI Arguments

| Flag | Default | Description |
|------|---------|-------------|
| `--region` | us-east-1 | AWS region |
| `--connection` | default | Snowflake CLI connection name |
| `--skip-deploy` | false | Reuse existing AWS infra (Lambda, API GW, DynamoDB) |
| `--skip-setup` | false | Reuse existing Snowflake objects |
| `--cleanup` | false | Tear down all resources and exit |
| `--delay-ms` | 0 | Simulated API latency in Lambda (milliseconds) |
| `--quick` | false | Reduced matrix: XS/M/XL warehouses, 10k/100k tables, default/1000 batch |

## Test Matrix

**Warehouses**: XSMALL, SMALL, MEDIUM, LARGE, XLARGE, XXLARGE
**Tables**: 10K, 100K, 1M rows of UUID tokens
**Batch configs**: default, MAX_BATCH_ROWS=100/500/1000/5000
**Full matrix**: 6 × 3 × 5 = 90 benchmark queries

## Architecture

```
Snowflake Query
  → External Function (Snowflake batches rows automatically)
    → API Gateway (Regional REST, AWS_IAM auth)
      → Lambda (Go, provided.al2023)
        → Returns DETOK_ + token
        → Logs to CloudWatch + DynamoDB (fire-and-forget)
```

## Output

1. **Throughput table**: wall-clock duration and rows/sec for each combination
2. **Pivot table**: rows/sec by warehouse × batch config
3. **External function details**: Snowflake's own invocation counts, sent rows, avg batch size
4. **CloudWatch metrics**: peak concurrent Lambda executions, invocations/min, throttles
5. **Cost analysis**: estimated credits per million rows

All results are also stored in `EXT_FUNC_BENCHMARK.BENCHMARK.benchmark_results` for further analysis.

## Iterative Testing

Run the full setup once, then iterate with different latencies:

```bash
# First run — deploys everything
./run_benchmark.sh --quick

# Add 50ms simulated latency (skip infra setup)
./run_benchmark.sh --quick --skip-deploy --skip-setup --delay-ms 50

# Add 100ms simulated latency
./run_benchmark.sh --quick --skip-deploy --skip-setup --delay-ms 100

# Compare results across delays
snow sql -q "SELECT simulated_delay_ms, warehouse_name, row_count, batch_config,
  ROUND(rows_per_second) AS rps FROM EXT_FUNC_BENCHMARK.BENCHMARK.benchmark_results
  ORDER BY simulated_delay_ms, warehouse_name, row_count"
```

Note: When changing `--delay-ms` without `--skip-deploy`, the Lambda environment variable is updated automatically.

## Cleanup

```bash
./run_benchmark.sh --cleanup
```

Removes: Snowflake database + warehouses + integration, Lambda, API Gateway, DynamoDB table, IAM roles.
