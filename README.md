# Snowflake External Function Throughput Benchmark

Measures how Snowflake batches and parallelizes external function calls to determine detokenization throughput ceilings — in both mock mode (simulated latency) and live Skyflow mode.

## What This Measures

- **Batch size**: How many rows does Snowflake send per Lambda invocation? (confirmed: 4,096)
- **Concurrency**: How many Lambdas does Snowflake run in parallel per warehouse size?
- **Throughput**: Raw rows/sec at each warehouse size, table size, and latency profile
- **Warehouse scaling**: Does bigger warehouse = more external function parallelism?
- **Skyflow integration**: End-to-end tokenize/detokenize throughput via Skyflow APIs

## Prerequisites

- **AWS CLI** configured with credentials (`aws sts get-caller-identity`)
- **SnowSQL** configured with a connection (`snowsql -c default -q "SELECT 1"`)
- **Go** 1.21+ (`go version`)
- **jq** (`brew install jq`)

Or run `./run_benchmark.sh --install-prereqs` to install missing tools via Homebrew/pip.

## Configuration

Create `benchmark.conf` in this directory. All credentials and IDs are read from this file — not CLI flags.

```bash
# AWS
AWS_PROFILE="your-aws-profile"
REGION="us-east-2"

# Snowflake
SF_CONNECTION="default"

# Skyflow
SKYFLOW_URL="https://your-vault.skyvault.skyflowapis.com"
SKYFLOW_API_KEY="your-api-key"
SKYFLOW_VAULT_ID="your-vault-id"
SKYFLOW_ACCOUNT_ID="your-account-id"
SKYFLOW_TABLE="table1"
SKYFLOW_COLUMN="name"
SKYFLOW_BATCH_SIZE=25
SKYFLOW_CONCURRENCY=50
```

### Config variables

| Variable | Description |
|----------|-------------|
| `AWS_PROFILE` | AWS CLI named profile for deploying Lambda, API Gateway, and IAM resources |
| `REGION` | AWS region for all resources (Lambda, API Gateway, CloudWatch). Must match Snowflake's network path for lowest latency |
| `SF_CONNECTION` | SnowSQL connection name (as defined in `~/.snowsql/config`) |
| `SKYFLOW_URL` | Skyflow Data Plane URL (e.g. `https://<id>.skyvault.skyflowapis.com`). When set and `--mock` is not passed, the Lambda calls Skyflow APIs instead of returning mock data |
| `SKYFLOW_API_KEY` | Skyflow API key (JWT) for authenticating with the Skyflow vault |
| `SKYFLOW_VAULT_ID` | Skyflow vault ID containing the tokenized data |
| `SKYFLOW_ACCOUNT_ID` | Skyflow account ID for API authentication |
| `SKYFLOW_TABLE` | Skyflow vault table name to tokenize/detokenize against |
| `SKYFLOW_COLUMN` | Skyflow vault column name for the token field |
| `SKYFLOW_BATCH_SIZE` | Number of tokens per Skyflow API call. The Lambda batches tokens from each Snowflake batch (4,096 rows) into sub-batches of this size. Lower = more API calls but less per-call latency. Tune based on Skyflow rate limits |
| `SKYFLOW_CONCURRENCY` | Max parallel Skyflow API calls per Lambda invocation. Controls how many sub-batches are sent concurrently. Higher = faster per-Lambda throughput but more load on Skyflow. Tune alongside `SKYFLOW_BATCH_SIZE` |

## Quick Start

```bash
# First run — deploys Lambda, API Gateway, Snowflake objects, seed tables
./run_benchmark.sh --quick

# Probe mode — measure pipeline fundamentals (batch size, concurrency, throughput)
./run_benchmark.sh --probe --skip-deploy --skip-setup

# Live Skyflow benchmarks (uses SKYFLOW_* config from benchmark.conf)
./run_benchmark.sh --quick --skip-deploy --skip-setup

# Mock mode (force mock even when Skyflow config is present)
./run_benchmark.sh --quick --skip-deploy --skip-setup --mock

# Cleanup everything
./run_benchmark.sh --cleanup
```

## CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--skip-deploy` | false | Reuse existing AWS infra (Lambda, API Gateway) |
| `--skip-setup` | false | Reuse existing Snowflake objects (warehouses, tables, functions) |
| `--cleanup` | false | Tear down all resources and exit |
| `--delay-ms MS` | 0 | Simulated API latency in Lambda (milliseconds) |
| `--iterations N` | 3 | Measured runs per warehouse/table combo |
| `--concurrency N` | 2900 | Lambda reserved concurrency limit |
| `--mock` | false | Force mock mode (ignore Skyflow config) |
| `--install-prereqs` | false | Install missing prerequisites via Homebrew/pip |
| `--validate-10b` | false | Append XL/2XL x 10B validation after main matrix |

### Test matrix presets

| Flag | Warehouses | Tables | Iterations | Use case |
|------|------------|--------|------------|----------|
| *(default)* | XS, M, XL, 2XL | 100M, 1B | 3 | Full production benchmark |
| `--quick` | XL | 10M, 100M | 3 | Fast iteration on a single warehouse |
| `--medium` | XL | 1M, 10M | 2 | Mid-size Skyflow validation |
| `--micro` | XL | 1K, 10K, 100K | 1 | Skyflow integration smoke test |
| `--probe` | XS, L, XL | 100M | 1 | Pipeline fundamentals (mock-only) |

## Modes

### Mock mode

When `SKYFLOW_URL` is not set or `--mock` is passed, the Lambda returns `DETOK_<token>` with optional simulated delay (`--delay-ms`). Useful for measuring the Snowflake-to-Lambda pipeline without Skyflow latency.

### Skyflow mode

When `SKYFLOW_URL` is set in `benchmark.conf` (and `--mock` is not passed), the Lambda calls Skyflow APIs for real tokenize/detokenize operations. The Snowflake external function passes an `x-operation` header (`tokenize` or `detokenize`) to control behavior.

### Probe mode (`--probe`)

Measures the three pipeline fundamentals without Skyflow:

1. **Batch size** — confirmed at 4,096 rows per Lambda invocation
2. **Concurrency** — peak concurrent Lambda executions per warehouse size (via CloudWatch ConcurrentExecutions polling)
3. **Throughput** — rows/sec = total rows / query elapsed time

Probe runs each query to completion, polls CloudWatch every 5s to track peak concurrency, then waits for concurrent executions to drop to 0 before starting the next query. This ensures clean, isolated measurements per warehouse.

## Architecture

```
Snowflake Query
  → External Function (Snowflake batches rows at 4,096 per invocation)
    → API Gateway (Regional REST, AWS_IAM auth)
      → Lambda (Go, provided.al2023, 128MB)
        → Mock mode: returns DETOK_<token> (optional delay)
        → Skyflow mode: calls Skyflow tokenize/detokenize API
        → Logs METRIC lines to CloudWatch
```

## Phases

The script runs in 5 phases:

1. **Preflight checks** — validates AWS, SnowSQL, Go, jq
2. **AWS deployment** — builds Lambda, creates API Gateway, configures IAM (skipped with `--skip-deploy`)
3. **Snowflake setup** — creates warehouses, tables, seeds token data, creates external functions (skipped with `--skip-setup`)
4. **Benchmarks** — runs the test matrix with warmup + measured iterations (or probe concurrency polling)
5. **Results summary** — CloudWatch log analysis, pipeline analysis table, CW metrics

## Output

- **Pipeline analysis table**: invocations, batch sizes, CW peak concurrency, throughput per combo
- **CloudWatch metrics**: peak concurrent executions, invocations/min, throttles over last 60 min
- **Snowflake results table**: `EXT_FUNC_BENCHMARK.BENCHMARK.benchmark_results`

## Iterative Testing

Deploy once, then iterate:

```bash
# First run — full deploy
./run_benchmark.sh --quick

# Iterate with different latencies (reuse infra)
./run_benchmark.sh --quick --skip-deploy --skip-setup --delay-ms 50
./run_benchmark.sh --quick --skip-deploy --skip-setup --delay-ms 100

# Probe pipeline fundamentals
./run_benchmark.sh --probe --skip-deploy --skip-setup

# Compare results
snowsql -c default -q "
  SELECT warehouse_name, row_count, simulated_delay_ms,
    ROUND(rows_per_second) AS rps
  FROM EXT_FUNC_BENCHMARK.BENCHMARK.benchmark_results
  ORDER BY simulated_delay_ms, warehouse_name, row_count
"
```

## Cleanup

```bash
./run_benchmark.sh --cleanup
```

Removes: Snowflake database + warehouses + API integration, Lambda, API Gateway, IAM roles.
