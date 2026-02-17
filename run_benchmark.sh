#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# Snowflake External Function Throughput Benchmark
#
# Measures how Snowflake batches and parallelizes external function calls.
# Single script — zero manual steps.
#
# Phase 4 overhaul: statistical rigor (multiple iterations),
# dynamic concurrency, warmup passes, optional 10B validation.
#
# Usage: ./run_benchmark.sh [options]
###############################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ─── Config file (required) ──────────────────────────────────────────────────
CONF_FILE="${SCRIPT_DIR}/benchmark.conf"
if [[ ! -f "$CONF_FILE" ]]; then
  echo "ERROR: Config file not found: ${CONF_FILE}"
  echo "Create benchmark.conf with required settings. See benchmark.conf.example."
  exit 1
fi
source "$CONF_FILE"

# Validate required config values
_require_conf() {
  local var_name="$1"
  if [[ -z "${!var_name:-}" ]]; then
    echo "ERROR: ${var_name} not set in ${CONF_FILE}"
    exit 1
  fi
}
_require_conf REGION
_require_conf AWS_PROFILE
_require_conf SF_CONNECTION
_require_conf SKYFLOW_URL
_require_conf SKYFLOW_API_KEY
_require_conf SKYFLOW_VAULT_ID
_require_conf SKYFLOW_ACCOUNT_ID
_require_conf SKYFLOW_TABLE
_require_conf SKYFLOW_COLUMN
_require_conf SKYFLOW_BATCH_SIZE
_require_conf SKYFLOW_CONCURRENCY

# ─── Runtime flags (not in config file) ──────────────────────────────────────
SKIP_DEPLOY=false
SKIP_SETUP=false
CLEANUP=false
DELAY_MS="${DELAY_MS:-0}"
QUICK=false
MICRO=false
MEDIUM=false
INSTALL_PREREQS=false
ITERATIONS="${ITERATIONS:-3}"
CONCURRENCY="${CONCURRENCY:-4900}"
VALIDATE_10B=false
FORCE_MOCK=false
PROBE=false
CUSTOM_ROWS="${CUSTOM_ROWS:-}"
CUSTOM_UNIQUE_TOKENS="${CUSTOM_UNIQUE_TOKENS:-}"
CUSTOM_WAREHOUSE="${CUSTOM_WAREHOUSE:-}"

# Resource naming
# AWS resources use hyphens, Snowflake uses underscores (SF rejects hyphens in identifiers)
AWS_PREFIX="ext-func-bench"
SF_PREFIX="ext_func_bench"
LAMBDA_NAME="${AWS_PREFIX}-detokenize"
API_NAME="${AWS_PREFIX}-api"
LAMBDA_ROLE_NAME="${AWS_PREFIX}-lambda-role"
SF_ROLE_NAME="${AWS_PREFIX}-sf-role"
SF_DB="EXT_FUNC_BENCHMARK"
SF_SCHEMA="BENCHMARK"

# Warehouse definitions (bash 3.2 compatible — no associative arrays)
wh_size() {
  case $1 in
    BENCH_XS)  echo XSMALL ;;
    BENCH_S)   echo SMALL ;;
    BENCH_M)   echo MEDIUM ;;
    BENCH_L)   echo LARGE ;;
    BENCH_XL)  echo XLARGE ;;
    BENCH_2XL) echo XXLARGE ;;
    BENCH_3XL) echo XXXLARGE ;;
    BENCH_4XL) echo X4LARGE ;;
  esac
}
wh_credits() {
  case $1 in
    BENCH_XS)  echo 1 ;;
    BENCH_S)   echo 2 ;;
    BENCH_M)   echo 4 ;;
    BENCH_L)   echo 8 ;;
    BENCH_XL)  echo 16 ;;
    BENCH_2XL) echo 32 ;;
    BENCH_3XL) echo 64 ;;
    BENCH_4XL) echo 128 ;;
  esac
}

ALL_WAREHOUSES=(BENCH_XS BENCH_M BENCH_XL BENCH_2XL)
ALL_TABLES=(test_tokens_100m test_tokens_1b)

TABLE_ROWS_test_tokens_1k=1000
TABLE_ROWS_test_tokens_10k=10000
TABLE_ROWS_test_tokens_100k=100000
TABLE_ROWS_test_tokens_1m=1000000
TABLE_ROWS_test_tokens_10m=10000000
TABLE_ROWS_test_tokens_100m=100000000
TABLE_ROWS_test_tokens_500m=500000000
TABLE_ROWS_test_tokens_1b=1000000000
TABLE_ROWS_test_tokens_10b=10000000000

# ─── Parse arguments ─────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case $1 in
    --skip-deploy) SKIP_DEPLOY=true; shift ;;
    --skip-setup)  SKIP_SETUP=true; shift ;;
    --cleanup)     CLEANUP=true; shift ;;
    --delay-ms)    DELAY_MS="$2"; shift 2 ;;
    --quick)           QUICK=true; shift ;;
    --medium)          MEDIUM=true; shift ;;
    --micro)           MICRO=true; shift ;;
    --install-prereqs) INSTALL_PREREQS=true; shift ;;
    --iterations)  ITERATIONS="$2"; shift 2 ;;
    --concurrency) CONCURRENCY="$2"; shift 2 ;;
    --validate-10b) VALIDATE_10B=true; shift ;;
    --mock)                FORCE_MOCK=true; shift ;;
    --probe)               PROBE=true; shift ;;
    --rows)            CUSTOM_ROWS="$2"; shift 2 ;;
    --unique-tokens)   CUSTOM_UNIQUE_TOKENS="$2"; shift 2 ;;
    --warehouse)       CUSTOM_WAREHOUSE="$2"; shift 2 ;;
    -h|--help)
      echo "Usage: $0 [options]"
      echo "  All credentials and IDs are read from benchmark.conf"
      echo ""
      echo "  --skip-deploy          Reuse existing AWS and Snowflake infrastructure (Lambda, API Gateway, IAM, API integration, external functions, warehouses)"
      echo "  --skip-setup           Reuse existing Snowflake data (tables, token seeding, results table)"
      echo "  --cleanup              Tear down everything and exit"
      echo "  --delay-ms MS          Simulated API latency in Lambda (default: 0)"
      echo "  --quick                Reduced test matrix (XL x 10M/100M x 3 iters)"
      echo "  --medium               Medium test matrix (XL x 1M/10M x 2 iters, 2.5M seeds)"
      echo "  --micro                Small test matrix (XL x 1K/10K/100K x 1 iter, 25K seeds)"
      echo "  --install-prereqs      Install missing prerequisites via Homebrew/pip"
      echo "  --iterations N         Measured runs per combo (default: 3)"
      echo "  --concurrency N        Lambda reserved concurrency (default: 4900)"
      echo "  --validate-10b         Append XL/2XL x 10B x 1 iteration after main matrix"
      echo "  --mock                 Force mock mode (ignore Skyflow config)"
      echo "  --rows N               Custom table with N rows (overrides tier table selection)"
      echo "  --unique-tokens N      Custom unique token count for Skyflow seeding (overrides tier default)"
      echo "  --warehouse SIZE       Warehouse size: XS, S, M, L, XL, 2XL, 3XL, 4XL (overrides tier)"
      echo "  --probe                Probe mode: measure pipeline fundamentals (batch size,"
      echo "                         concurrency, throughput) with mock-only, time-bounded tests"
      exit 0
      ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# Probe mode: measure pipeline fundamentals (batch size, concurrency, throughput)
# Mock-only, time-bounded tests. No Skyflow calls.
if $PROBE; then
  ALL_WAREHOUSES=(BENCH_XS BENCH_L BENCH_XL)
  ALL_TABLES=(test_tokens_100m)
  ITERATIONS=1
  FORCE_MOCK=true
# Micro mode: XL, small tables, 1 iteration — validate Skyflow with realistic dedup
elif $MICRO; then
  ALL_WAREHOUSES=(BENCH_XL)
  ALL_TABLES=(test_tokens_1k test_tokens_10k test_tokens_100k)
  ITERATIONS=1
elif $MEDIUM; then
  ALL_WAREHOUSES=(BENCH_XL)
  ALL_TABLES=(test_tokens_1m test_tokens_10m)
  ITERATIONS=2
elif $QUICK; then
  ALL_WAREHOUSES=(BENCH_XL)
  ALL_TABLES=(test_tokens_10m test_tokens_100m)
fi

# Custom warehouse override (--warehouse SIZE)
if [[ -n "$CUSTOM_WAREHOUSE" ]]; then
  case "$CUSTOM_WAREHOUSE" in
    XS)  ALL_WAREHOUSES=(BENCH_XS) ;;
    S)   ALL_WAREHOUSES=(BENCH_S) ;;
    M)   ALL_WAREHOUSES=(BENCH_M) ;;
    L)   ALL_WAREHOUSES=(BENCH_L) ;;
    XL)  ALL_WAREHOUSES=(BENCH_XL) ;;
    2XL) ALL_WAREHOUSES=(BENCH_2XL) ;;
    3XL) ALL_WAREHOUSES=(BENCH_3XL) ;;
    4XL) ALL_WAREHOUSES=(BENCH_4XL) ;;
    *)   echo "ERROR: --warehouse must be one of: XS, S, M, L, XL, 2XL, 3XL, 4XL"; exit 1 ;;
  esac
fi

# Custom rows override (--rows N)
if [[ -n "$CUSTOM_ROWS" ]]; then
  if ! [[ "$CUSTOM_ROWS" =~ ^[0-9]+$ ]] || [[ "$CUSTOM_ROWS" -lt 1 ]]; then
    echo "ERROR: --rows must be a positive integer"; exit 1
  fi
  ALL_TABLES=(test_tokens_custom)
  TABLE_ROWS_test_tokens_custom="$CUSTOM_ROWS"
fi

# Skyflow mode determination
SKYFLOW_MODE=false
if [[ -n "$SKYFLOW_URL" ]] && ! $FORCE_MOCK; then
  SKYFLOW_MODE=true
fi

# ─── Helpers ──────────────────────────────────────────────────────────────────
log()  { echo -e "\033[1;34m[$(date '+%H:%M:%S')]\033[0m $*"; }
ok()   { echo -e "\033[1;32m  ✓\033[0m $*"; }
warn() { echo -e "\033[1;33m  ⚠\033[0m $*"; }
err()  { echo -e "\033[1;31m  ✗\033[0m $*"; }
die()  { err "$*"; exit 1; }

SNOWSQL_OPTS=(-c "$SF_CONNECTION" --noup -o friendly=false -o timing=false)

snow_sql() {
  snowsql "${SNOWSQL_OPTS[@]}" -q "$1" "${@:2}" 2>/dev/null
}

# Silent version — suppresses all output, for DDL/DML where we don't need results
snow_sql_silent() {
  snowsql "${SNOWSQL_OPTS[@]}" -o quiet=true -q "$1" "${@:2}" > /dev/null 2>&1
}

snow_sql_json() {
  # Run snowsql with JSON output, then extract just the JSON array
  local raw
  raw=$(snowsql "${SNOWSQL_OPTS[@]}" -q "$1" -o output_format=json 2>/dev/null)
  # Extract the JSON array — find the line containing [ and print it
  # snowsql may emit the entire JSON on a single line
  echo "$raw" | grep '^\[' | head -1
}

aws_() {
  if [[ -n "$AWS_PROFILE" ]]; then
    aws --region "$REGION" --profile "$AWS_PROFILE" "$@"
  else
    aws --region "$REGION" "$@"
  fi
}

get_account_id() {
  aws_ sts get-caller-identity --query Account --output text
}

nanos() {
  # macOS-compatible nanosecond timestamp
  if command -v gdate &>/dev/null; then
    gdate +%s%N
  elif [[ "$(uname)" == "Darwin" ]]; then
    python3 -c "import time; print(int(time.time() * 1e9))"
  else
    date +%s%N
  fi
}

millis_since() {
  local start=$1
  local now
  now=$(nanos)
  echo $(( (now - start) / 1000000 ))
}

# ─── Install prerequisites ────────────────────────────────────────────────────
do_install_prereqs() {
  log "INSTALL: Checking and installing prerequisites..."
  echo ""

  NEED_CONFIG=()

  # ── Homebrew (macOS) ──
  if [[ "$(uname)" == "Darwin" ]] && ! command -v brew &>/dev/null; then
    log "Installing Homebrew..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    ok "Homebrew installed"
  fi

  # ── jq ──
  if ! command -v jq &>/dev/null; then
    log "Installing jq..."
    if [[ "$(uname)" == "Darwin" ]]; then
      brew install jq
    else
      sudo apt-get update -qq && sudo apt-get install -y -qq jq
    fi
    ok "jq installed"
  else
    ok "jq already installed"
  fi

  # ── Go ──
  if ! command -v go &>/dev/null; then
    log "Installing Go..."
    if [[ "$(uname)" == "Darwin" ]]; then
      brew install go
    else
      sudo apt-get update -qq && sudo apt-get install -y -qq golang
    fi
    ok "Go installed"
  else
    ok "Go already installed ($(go version | awk '{print $3}'))"
  fi

  # ── AWS CLI ──
  if ! command -v aws &>/dev/null; then
    log "Installing AWS CLI..."
    if [[ "$(uname)" == "Darwin" ]]; then
      brew install awscli
    else
      curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
      unzip -qo /tmp/awscliv2.zip -d /tmp && sudo /tmp/aws/install
      rm -rf /tmp/awscliv2.zip /tmp/aws
    fi
    ok "AWS CLI installed"
  else
    ok "AWS CLI already installed ($(aws --version 2>&1 | awk '{print $1}'))"
  fi

  # ── SnowSQL ──
  if ! command -v snowsql &>/dev/null; then
    log "Installing SnowSQL..."
    if [[ "$(uname)" == "Darwin" ]]; then
      brew install --cask snowflake-snowsql
    else
      curl -O https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/1.4/linux_x86_64/snowsql-1.4.5-linux_x86_64.bash
      bash snowsql-1.4.5-linux_x86_64.bash && rm -f snowsql-1.4.5-linux_x86_64.bash
    fi
    ok "SnowSQL installed"
  else
    ok "SnowSQL already installed ($(snowsql --version 2>&1 | head -1))"
  fi

  echo ""

  # ── Check AWS credentials ──
  log "Checking AWS credentials..."
  if aws sts get-caller-identity > /dev/null 2>&1; then
    AWS_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null)
    ok "AWS authenticated (account: ${AWS_ID})"
  else
    warn "AWS not configured"
    echo ""
    echo "  Run 'aws configure' and provide:"
    echo "    AWS Access Key ID"
    echo "    AWS Secret Access Key"
    echo "    Default region (e.g., us-east-1)"
    echo ""
    read -rp "  Would you like to run 'aws configure' now? [y/N] " ans
    if [[ "$ans" =~ ^[Yy] ]]; then
      aws configure
      if aws sts get-caller-identity > /dev/null 2>&1; then
        ok "AWS configured successfully"
      else
        err "AWS configuration failed — check your credentials"
        NEED_CONFIG+=(aws)
      fi
    else
      NEED_CONFIG+=(aws)
    fi
  fi

  echo ""

  # ── Check SnowSQL connection ──
  log "Checking SnowSQL connection '${SF_CONNECTION}'..."
  if snowsql -c "$SF_CONNECTION" -q "SELECT 1" > /dev/null 2>&1; then
    ok "SnowSQL connection '${SF_CONNECTION}' is working"
  else
    warn "SnowSQL connection '${SF_CONNECTION}' not configured or not working"
    echo ""
    echo "  SnowSQL needs a connection in ~/.snowsql/config."
    echo ""
    read -rp "  Would you like to configure it now? [y/N] " ans
    if [[ "$ans" =~ ^[Yy] ]]; then
      echo ""
      read -rp "  Snowflake account identifier (e.g., xy12345.us-east-1): " sf_account
      read -rp "  Username: " sf_user
      read -rsp "  Password: " sf_pass
      echo ""
      read -rp "  Warehouse (e.g., COMPUTE_WH): " sf_wh
      read -rp "  Role (default: ACCOUNTADMIN): " sf_role
      sf_role=${sf_role:-ACCOUNTADMIN}

      # Append connection to config
      CONFIG_FILE="${HOME}/.snowsql/config"
      mkdir -p "$(dirname "$CONFIG_FILE")"
      {
        echo ""
        echo "[connections.${SF_CONNECTION}]"
        echo "accountname = ${sf_account}"
        echo "username = ${sf_user}"
        echo "password = ${sf_pass}"
        echo "warehousename = ${sf_wh}"
        echo "rolename = ${sf_role}"
      } >> "$CONFIG_FILE"
      chmod 600 "$CONFIG_FILE"
      ok "Connection '${SF_CONNECTION}' added to ${CONFIG_FILE}"

      # Test it
      echo ""
      log "Testing connection..."
      if snowsql -c "$SF_CONNECTION" -q "SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_ROLE()" 2>/dev/null; then
        ok "SnowSQL connection is working"
      else
        err "Connection test failed — check your credentials and account identifier"
        NEED_CONFIG+=(snowsql)
      fi
    else
      NEED_CONFIG+=(snowsql)
    fi
  fi

  echo ""

  # ── Summary ──
  if [[ ${#NEED_CONFIG[@]} -eq 0 ]]; then
    log "All prerequisites installed and configured. Ready to run:"
    echo "  $0"
    echo "  $0 --quick    # reduced test matrix for faster first run"
  else
    log "Prerequisites installed, but configuration still needed:"
    for item in "${NEED_CONFIG[@]}"; do
      case $item in
        aws)     echo "  - AWS: run 'aws configure'" ;;
        snowsql) echo "  - SnowSQL: edit ~/.snowsql/config (see above)" ;;
      esac
    done
    echo ""
    echo "  After configuring, re-run: $0 --install-prereqs"
  fi
}

if $INSTALL_PREREQS; then
  do_install_prereqs
  exit 0
fi

# ─── Phase 0: Cleanup ────────────────────────────────────────────────────────
do_cleanup() {
  log "CLEANUP: Tearing down all resources..."

  log "Dropping Snowflake objects..."
  snow_sql_silent "DROP DATABASE IF EXISTS ${SF_DB}" || true
  for wh in "${ALL_WAREHOUSES[@]}"; do
    snow_sql_silent "DROP WAREHOUSE IF EXISTS ${wh}" || true
  done
  snow_sql_silent "DROP INTEGRATION IF EXISTS ${SF_PREFIX}_api_integration" || true
  ok "Snowflake objects dropped"

  log "Deleting API Gateway..."
  local api_id
  api_id=$(aws_ apigateway get-rest-apis --query "items[?name=='${API_NAME}'].id | [0]" --output text 2>/dev/null || true)
  if [[ "$api_id" =~ ^[a-z0-9]+$ ]]; then
    aws_ apigateway delete-rest-api --rest-api-id "$api_id" 2>/dev/null || true
    ok "API Gateway deleted"
  else
    ok "API Gateway not found (already deleted)"
  fi

  log "Deleting Lambda function..."
  if aws_ lambda delete-function --function-name "$LAMBDA_NAME" > /dev/null 2>&1; then
    ok "Lambda deleted"
  else
    warn "Lambda delete failed or not found (check --profile flag)"
  fi

  log "Cleaning up IAM roles..."
  # Lambda role
  aws_ iam delete-role-policy --role-name "$LAMBDA_ROLE_NAME" --policy-name "${LAMBDA_ROLE_NAME}-policy" 2>/dev/null || true
  aws_ iam delete-role --role-name "$LAMBDA_ROLE_NAME" 2>/dev/null || true
  # Snowflake role
  aws_ iam delete-role-policy --role-name "$SF_ROLE_NAME" --policy-name "${SF_ROLE_NAME}-policy" 2>/dev/null || true
  aws_ iam delete-role --role-name "$SF_ROLE_NAME" 2>/dev/null || true
  ok "IAM roles deleted"

  log "Cleanup complete."
}

if $CLEANUP; then
  do_cleanup
  exit 0
fi

###############################################################################
# Phase 1: Preflight Checks
###############################################################################
log "PHASE 1: Preflight checks"

# AWS
aws_ sts get-caller-identity > /dev/null 2>&1 || die "AWS CLI not configured or credentials expired. Run 'aws sts get-caller-identity' to debug."
AWS_ACCOUNT_ID=$(get_account_id)
ok "AWS authenticated (account: ${AWS_ACCOUNT_ID})"

# Snowflake
command -v snowsql &>/dev/null || die "snowsql is not installed. Run '$0 --install-prereqs' or visit https://docs.snowflake.com/en/user-guide/snowsql-install-config"
snow_sql_silent "SELECT 1" || die "SnowSQL cannot connect. Run 'snowsql -c ${SF_CONNECTION} -q \"SELECT 1\"' to debug. Check ~/.snowsql/config for [connections.${SF_CONNECTION}]."
ok "SnowSQL connected (connection: ${SF_CONNECTION})"

# Go
command -v go &>/dev/null || die "Go is not installed. Install from https://go.dev"
ok "Go $(go version | awk '{print $3}')"

# jq
command -v jq &>/dev/null || die "jq is not installed. Install with 'brew install jq'"
ok "jq installed"

echo ""

###############################################################################
# Phase 2: Deploy AWS Infrastructure
###############################################################################
if $SKIP_DEPLOY; then
  log "PHASE 2: Skipping AWS deployment (--skip-deploy)"
  # Still need to discover the API Gateway URL
  API_ID=$(aws_ apigateway get-rest-apis --query "items[?name=='${API_NAME}'].id | [0]" --output text 2>/dev/null || true)
  if [[ ! "$API_ID" =~ ^[a-z0-9]+$ ]]; then
    die "Cannot find existing API Gateway '${API_NAME}'. Run without --skip-deploy first."
  fi
  API_URL="https://${API_ID}.execute-api.${REGION}.amazonaws.com/prod"
  SF_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${SF_ROLE_NAME}"
  ok "Using existing API: ${API_URL}"
else
  log "PHASE 2: Deploying AWS infrastructure"

  # ── Build Lambda ──
  log "Building Go Lambda binary..."
  (
    cd "${SCRIPT_DIR}/lambda"
    GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o bootstrap .
  )
  (cd "${SCRIPT_DIR}/lambda" && zip -j "${SCRIPT_DIR}/lambda.zip" bootstrap) > /dev/null
  rm -f "${SCRIPT_DIR}/lambda/bootstrap"
  ok "Lambda binary built"

  # ── Lambda execution role ──
  log "Creating Lambda execution role..."
  LAMBDA_TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  aws_ iam create-role \
    --role-name "$LAMBDA_ROLE_NAME" \
    --assume-role-policy-document "$LAMBDA_TRUST" \
    > /dev/null 2>&1 || warn "Lambda role may already exist"

  LAMBDA_POLICY=$(cat <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:${REGION}:${AWS_ACCOUNT_ID}:*"
    }
  ]
}
POLICY
)
  aws_ iam put-role-policy \
    --role-name "$LAMBDA_ROLE_NAME" \
    --policy-name "${LAMBDA_ROLE_NAME}-policy" \
    --policy-document "$LAMBDA_POLICY" \
    > /dev/null 2>&1
  LAMBDA_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${LAMBDA_ROLE_NAME}"
  ok "Lambda role: ${LAMBDA_ROLE_NAME}"

  # Wait for IAM propagation
  log "Waiting for IAM role propagation..."
  sleep 10
  ok "IAM propagation wait complete"

  # ── Deploy Lambda ──
  log "Deploying Lambda function..."

  # Skyflow env vars for Lambda
  LAMBDA_SKYFLOW_ENV=""
  if $SKYFLOW_MODE; then
    LAMBDA_SKYFLOW_ENV=",SKYFLOW_DATA_PLANE_URL=${SKYFLOW_URL}"
    LAMBDA_SKYFLOW_ENV+=",SKYFLOW_API_KEY=${SKYFLOW_API_KEY}"
    LAMBDA_SKYFLOW_ENV+=",SKYFLOW_VAULT_ID=${SKYFLOW_VAULT_ID}"
    LAMBDA_SKYFLOW_ENV+=",SKYFLOW_ACCOUNT_ID=${SKYFLOW_ACCOUNT_ID}"
    LAMBDA_SKYFLOW_ENV+=",SKYFLOW_TABLE_NAME=${SKYFLOW_TABLE}"
    LAMBDA_SKYFLOW_ENV+=",SKYFLOW_COLUMN_NAME=${SKYFLOW_COLUMN}"
    LAMBDA_SKYFLOW_ENV+=",SKYFLOW_BATCH_SIZE=${SKYFLOW_BATCH_SIZE}"
    LAMBDA_SKYFLOW_ENV+=",SKYFLOW_MAX_CONCURRENCY=${SKYFLOW_CONCURRENCY}"
  fi

  # Lambda memory/timeout — higher for Skyflow mode (real HTTP calls)
  LAMBDA_MEMORY=256
  LAMBDA_TIMEOUT=30
  if $SKYFLOW_MODE; then
    LAMBDA_MEMORY=512
    LAMBDA_TIMEOUT=120
  fi

  LAMBDA_ENV_VARS="Variables={SIMULATED_DELAY_MS=${DELAY_MS}${LAMBDA_SKYFLOW_ENV}}"

  # Try to create; if it exists, update
  if aws_ lambda create-function \
    --function-name "$LAMBDA_NAME" \
    --runtime provided.al2023 \
    --handler bootstrap \
    --role "$LAMBDA_ROLE_ARN" \
    --zip-file "fileb://${SCRIPT_DIR}/lambda.zip" \
    --memory-size "$LAMBDA_MEMORY" \
    --timeout "$LAMBDA_TIMEOUT" \
    --environment "$LAMBDA_ENV_VARS" \
    > /dev/null 2>&1; then
    ok "Lambda created: ${LAMBDA_NAME}"
  else
    aws_ lambda update-function-code \
      --function-name "$LAMBDA_NAME" \
      --zip-file "fileb://${SCRIPT_DIR}/lambda.zip" \
      > /dev/null 2>&1
    # Wait for update to complete before updating config
    aws_ lambda wait function-updated --function-name "$LAMBDA_NAME" 2>/dev/null || sleep 5
    aws_ lambda update-function-configuration \
      --function-name "$LAMBDA_NAME" \
      --memory-size "$LAMBDA_MEMORY" \
      --timeout "$LAMBDA_TIMEOUT" \
      --environment "$LAMBDA_ENV_VARS" \
      > /dev/null 2>&1
    ok "Lambda updated: ${LAMBDA_NAME}"
  fi

  if $SKYFLOW_MODE; then
    ok "Lambda mode: SKYFLOW (memory=${LAMBDA_MEMORY}MB, timeout=${LAMBDA_TIMEOUT}s)"
    ok "  Skyflow URL: ${SKYFLOW_URL}"
    ok "  Vault: ${SKYFLOW_VAULT_ID}, Table: ${SKYFLOW_TABLE}, Column: ${SKYFLOW_COLUMN}"
    ok "  Batch: ${SKYFLOW_BATCH_SIZE}, Concurrency: ${SKYFLOW_CONCURRENCY}"
  else
    ok "Lambda mode: MOCK"
  fi

  # ── Dynamic Lambda concurrency ──
  log "Setting Lambda concurrency..."
  ACCOUNT_LIMIT=$(aws_ lambda get-account-settings --query 'AccountLimit.ConcurrentExecutions' --output text 2>/dev/null || echo "1000")
  EFFECTIVE_CONCURRENCY=$CONCURRENCY
  MAX_ALLOWED=$(( ACCOUNT_LIMIT - 100 ))
  if [[ $EFFECTIVE_CONCURRENCY -gt $MAX_ALLOWED ]]; then
    warn "Requested concurrency ${CONCURRENCY} exceeds account limit (${ACCOUNT_LIMIT} - 100 reserved = ${MAX_ALLOWED})"
    EFFECTIVE_CONCURRENCY=$MAX_ALLOWED
  fi
  aws_ lambda put-function-concurrency \
    --function-name "$LAMBDA_NAME" \
    --reserved-concurrent-executions "$EFFECTIVE_CONCURRENCY" \
    > /dev/null
  ok "Lambda reserved concurrency: ${EFFECTIVE_CONCURRENCY} (account limit: ${ACCOUNT_LIMIT})"

  LAMBDA_ARN=$(aws_ lambda get-function --function-name "$LAMBDA_NAME" --query 'Configuration.FunctionArn' --output text)

  # ── API Gateway ──
  log "Creating API Gateway..."

  # Check if API already exists
  # AWS --output text returns "None" (possibly multiple times) when JMESPath yields null
  API_ID=$(aws_ apigateway get-rest-apis --query "items[?name=='${API_NAME}'].id | [0]" --output text 2>/dev/null || true)
  if [[ ! "$API_ID" =~ ^[a-z0-9]+$ ]]; then
    API_ID=$(aws_ apigateway create-rest-api \
      --name "$API_NAME" \
      --endpoint-configuration types=REGIONAL \
      --query 'id' --output text)
    ok "API Gateway created: ${API_ID}"
  else
    ok "API Gateway exists: ${API_ID}"
  fi

  # Get root resource ID
  ROOT_ID=$(aws_ apigateway get-resources --rest-api-id "$API_ID" --query 'items[?path==`/`].id | [0]' --output text)

  # Create /process resource (handles both tokenize and detokenize via header routing)
  RESOURCE_ID=$(aws_ apigateway get-resources --rest-api-id "$API_ID" --query "items[?pathPart=='process'].id | [0]" --output text 2>/dev/null || true)
  if [[ ! "$RESOURCE_ID" =~ ^[a-z0-9]+$ ]]; then
    RESOURCE_ID=$(aws_ apigateway create-resource \
      --rest-api-id "$API_ID" \
      --parent-id "$ROOT_ID" \
      --path-part process \
      --query 'id' --output text)
  fi
  ok "Resource /process: ${RESOURCE_ID}"

  # PUT method with AWS_IAM auth
  aws_ apigateway put-method \
    --rest-api-id "$API_ID" \
    --resource-id "$RESOURCE_ID" \
    --http-method POST \
    --authorization-type AWS_IAM \
    > /dev/null 2>&1 || true

  # Lambda proxy integration
  aws_ apigateway put-integration \
    --rest-api-id "$API_ID" \
    --resource-id "$RESOURCE_ID" \
    --http-method POST \
    --type AWS_PROXY \
    --integration-http-method POST \
    --uri "arn:aws:apigateway:${REGION}:lambda:path/2015-03-31/functions/${LAMBDA_ARN}/invocations" \
    > /dev/null 2>&1

  # Grant API Gateway permission to invoke Lambda
  aws_ lambda add-permission \
    --function-name "$LAMBDA_NAME" \
    --statement-id apigateway-invoke \
    --action lambda:InvokeFunction \
    --principal apigateway.amazonaws.com \
    --source-arn "arn:aws:execute-api:${REGION}:${AWS_ACCOUNT_ID}:${API_ID}/*" \
    > /dev/null 2>&1 || true

  # Deploy to prod stage
  aws_ apigateway create-deployment \
    --rest-api-id "$API_ID" \
    --stage-name prod \
    > /dev/null 2>&1
  ok "API deployed to prod stage"

  API_URL="https://${API_ID}.execute-api.${REGION}.amazonaws.com/prod"
  ok "API URL: ${API_URL}"

  # ── Snowflake-facing IAM role ──
  log "Creating Snowflake IAM role..."
  # Placeholder trust policy — will be updated after Snowflake integration is created
  PLACEHOLDER_TRUST=$(cat <<TRUST
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"AWS": "arn:aws:iam::${AWS_ACCOUNT_ID}:root"},
      "Action": "sts:AssumeRole",
      "Condition": {}
    }
  ]
}
TRUST
)
  aws_ iam create-role \
    --role-name "$SF_ROLE_NAME" \
    --assume-role-policy-document "$PLACEHOLDER_TRUST" \
    > /dev/null 2>&1 || warn "Snowflake IAM role may already exist"

  SF_API_POLICY=$(cat <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "execute-api:Invoke",
      "Resource": "arn:aws:execute-api:${REGION}:${AWS_ACCOUNT_ID}:${API_ID}/*"
    }
  ]
}
POLICY
)
  aws_ iam put-role-policy \
    --role-name "$SF_ROLE_NAME" \
    --policy-name "${SF_ROLE_NAME}-policy" \
    --policy-document "$SF_API_POLICY" \
    > /dev/null 2>&1
  SF_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${SF_ROLE_NAME}"
  ok "Snowflake IAM role: ${SF_ROLE_ARN}"

  # Cleanup zip
  rm -f "${SCRIPT_DIR}/lambda.zip"
fi

echo ""

###############################################################################
# Phase 3a: Snowflake Infrastructure
###############################################################################
FUNC_PREFIX="${SF_DB}.${SF_SCHEMA}"
INTEGRATION_NAME="${SF_PREFIX}_api_integration"

if $SKIP_DEPLOY; then
  log "PHASE 3a: Skipping Snowflake infrastructure (--skip-deploy)"
else
  log "PHASE 3a: Configuring Snowflake infrastructure"

  # ── Database and schema ──
  snow_sql_silent "CREATE DATABASE IF NOT EXISTS ${SF_DB}"
  snow_sql_silent "CREATE SCHEMA IF NOT EXISTS ${SF_DB}.${SF_SCHEMA}"
  ok "Database: ${SF_DB}.${SF_SCHEMA}"

  # ── API Integration ──
  log "Creating API integration..."

  # Snowflake replaces hyphens — use underscores in the integration name
  snow_sql_silent "CREATE OR REPLACE API INTEGRATION ${INTEGRATION_NAME}
    API_PROVIDER = aws_api_gateway
    API_AWS_ROLE_ARN = '${SF_ROLE_ARN}'
    API_ALLOWED_PREFIXES = ('${API_URL}/')
    ENABLED = true"
  ok "API integration created: ${INTEGRATION_NAME}"

  # ── Extract Snowflake's IAM identity ──
  log "Extracting Snowflake IAM identity from integration..."
  DESC_RAW=$(snow_sql_json "DESCRIBE INTEGRATION ${INTEGRATION_NAME}")

  # Helper: extract a property value from DESCRIBE INTEGRATION output.
  # Tries multiple strategies since snowsql JSON output format can vary.
  extract_integration_prop() {
    local prop_name="$1"
    local val=""

    # Strategy 1: Array of objects with "property"/"property_value" keys (any casing)
    val=$(echo "$DESC_RAW" | jq -r "
      [.. | objects | select(
        .property == \"${prop_name}\" or
        .PROPERTY == \"${prop_name}\" or
        .name == \"${prop_name}\"
      )] | .[0] |
      .property_value // .PROPERTY_VALUE // .value // .default // empty
    " 2>/dev/null || echo "")

    # Strategy 2: Grep fallback — look for the ARN/ID as a string value in the raw output
    if [[ -z "$val" || "$val" == "null" ]]; then
      if [[ "$prop_name" == "API_AWS_IAM_USER_ARN" ]]; then
        val=$(echo "$DESC_RAW" | grep -oE 'arn:aws:iam::[0-9]+:user/[^ "]+' | head -1 || echo "")
      elif [[ "$prop_name" == "API_AWS_EXTERNAL_ID" ]]; then
        # External ID follows the IAM ARN property in output; grab first non-ARN token-like value
        val=$(echo "$DESC_RAW" | grep -A1 "EXTERNAL_ID" | grep -oE '[A-Za-z0-9_=]+' | tail -1 || echo "")
      fi
    fi

    echo "$val"
  }

  set +e
  SF_IAM_USER_ARN=$(extract_integration_prop "API_AWS_IAM_USER_ARN")
  SF_EXTERNAL_ID=$(extract_integration_prop "API_AWS_EXTERNAL_ID")
  set -e

  if [[ -z "$SF_IAM_USER_ARN" || "$SF_IAM_USER_ARN" == "null" ]]; then
    err "Failed to extract API_AWS_IAM_USER_ARN from integration."
    echo "Raw DESCRIBE output:"
    echo "$DESC_RAW" | jq . 2>/dev/null || echo "$DESC_RAW"
    die "Cannot proceed without Snowflake's IAM user ARN."
  fi
  if [[ -z "$SF_EXTERNAL_ID" || "$SF_EXTERNAL_ID" == "null" ]]; then
    err "Failed to extract API_AWS_EXTERNAL_ID from integration."
    echo "Raw DESCRIBE output:"
    echo "$DESC_RAW" | jq . 2>/dev/null || echo "$DESC_RAW"
    die "Cannot proceed without Snowflake's external ID."
  fi

  ok "Snowflake IAM user ARN: ${SF_IAM_USER_ARN}"
  ok "Snowflake external ID:  ${SF_EXTERNAL_ID}"

  # ── Update AWS IAM trust policy with Snowflake identity ──
  log "Updating IAM role trust policy with Snowflake identity..."
  UPDATED_TRUST=$(cat <<TRUST
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"AWS": "${SF_IAM_USER_ARN}"},
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "${SF_EXTERNAL_ID}"
        }
      }
    }
  ]
}
TRUST
)
  aws_ iam update-assume-role-policy \
    --role-name "$SF_ROLE_NAME" \
    --policy-document "$UPDATED_TRUST"
  ok "Trust policy updated"

  # Wait for IAM propagation
  log "Waiting for IAM trust policy propagation..."
  sleep 15
  ok "IAM propagation wait complete"

  # ── External functions ──
  log "Creating external functions..."

  # Default mock function (backward compatible — always created)
  snow_sql_silent "CREATE OR REPLACE EXTERNAL FUNCTION ${FUNC_PREFIX}.benchmark_detokenize_default(token_value VARCHAR)
    RETURNS VARIANT
    API_INTEGRATION = ${INTEGRATION_NAME}
    HEADERS = ('sf-benchmark-config' = 'default')
    CONTEXT_HEADERS = (CURRENT_TIMESTAMP)
    AS '${API_URL}/process'"
  ok "Function: benchmark_detokenize_default"

  if $SKYFLOW_MODE; then
    # Skyflow tokenize function
    snow_sql_silent "CREATE OR REPLACE EXTERNAL FUNCTION ${FUNC_PREFIX}.benchmark_tokenize(value VARCHAR)
      RETURNS VARIANT
      API_INTEGRATION = ${INTEGRATION_NAME}
      HEADERS = ('X-Operation' = 'tokenize', 'X-Data-Type' = 'NAME', 'X-Vault-Type' = 'HIGH_PERF')
      CONTEXT_HEADERS = (CURRENT_TIMESTAMP)
      AS '${API_URL}/process'"
    ok "Function: benchmark_tokenize"

    # Skyflow detokenize function
    snow_sql_silent "CREATE OR REPLACE EXTERNAL FUNCTION ${FUNC_PREFIX}.benchmark_detokenize(token_value VARCHAR)
      RETURNS VARIANT
      API_INTEGRATION = ${INTEGRATION_NAME}
      HEADERS = ('X-Operation' = 'detokenize', 'X-Data-Type' = 'NAME', 'X-Vault-Type' = 'HIGH_PERF')
      CONTEXT_HEADERS = (CURRENT_TIMESTAMP)
      AS '${API_URL}/process'"
    ok "Function: benchmark_detokenize"
  fi

  # ── Warehouses (create before data gen so we can use a large WH for big tables) ──
  log "Creating benchmark warehouses..."
  for wh in "${ALL_WAREHOUSES[@]}"; do
    size="$(wh_size "$wh")"
    snow_sql_silent "CREATE WAREHOUSE IF NOT EXISTS ${wh}
      WAREHOUSE_SIZE = '${size}'
      AUTO_SUSPEND = 60
      AUTO_RESUME = TRUE
      INITIALLY_SUSPENDED = TRUE"
    ok "Warehouse: ${wh} (${size})"
  done
fi

###############################################################################
# Phase 3b: Snowflake Data Setup
###############################################################################
if $SKIP_SETUP; then
  log "PHASE 3b: Skipping Snowflake data setup (--skip-setup)"
  if [[ -n "$CUSTOM_ROWS" ]]; then
    warn "--rows specified with --skip-setup: table test_tokens_custom must already exist"
  fi
else
  log "PHASE 3b: Setting up Snowflake data"

  # ── Select largest warehouse for data generation ──
  DATA_GEN_WH=""
  for candidate in BENCH_4XL BENCH_3XL BENCH_2XL BENCH_XL BENCH_L BENCH_M BENCH_S BENCH_XS; do
    for wh in "${ALL_WAREHOUSES[@]}"; do
      if [[ "$wh" == "$candidate" ]]; then DATA_GEN_WH="$candidate"; break 2; fi
    done
  done
  log "Generating test data tables (using warehouse ${DATA_GEN_WH})..."

  # Determine all tables to generate (main matrix + 10B if requested)
  TABLES_TO_GEN=("${ALL_TABLES[@]}")
  if $VALIDATE_10B; then
    TABLES_TO_GEN+=(test_tokens_10b)
  fi

  for tbl in "${TABLES_TO_GEN[@]}"; do
    row_var="TABLE_ROWS_${tbl}"
    rows=${!row_var}

    # Use SEQ8() for tables > 4.3B rows (SEQ4() max is 2^32 - 1)
    if [[ $rows -gt 4294967295 ]]; then
      SEQ_FUNC="SEQ8()"
    else
      SEQ_FUNC="SEQ4()"
    fi

    log "  Generating ${tbl} (${rows} rows, using ${SEQ_FUNC})..."
    snow_sql_silent "USE WAREHOUSE ${DATA_GEN_WH};
      CREATE OR REPLACE TABLE ${FUNC_PREFIX}.${tbl} AS
      SELECT
        ${SEQ_FUNC} AS id,
        UUID_STRING() AS token_value,
        'extra_data_' || ${SEQ_FUNC}::VARCHAR AS extra_col
      FROM TABLE(GENERATOR(ROWCOUNT => ${rows}))"
    ok "Table: ${tbl} (${rows} rows)"
  done

  # ── Token seeding for Skyflow mode ──
  if $SKYFLOW_MODE; then
    # Seed count: 25% unique tokens of the largest table in each tier
    if $MICRO; then
      SEED_COUNT=25000       # 25% of 100K
    elif $MEDIUM; then
      SEED_COUNT=2500000     # 25% of 10M
    else
      SEED_COUNT=25000       # default for quick/full
    fi

    if [[ -n "$CUSTOM_UNIQUE_TOKENS" ]]; then
      if ! [[ "$CUSTOM_UNIQUE_TOKENS" =~ ^[0-9]+$ ]] || [[ "$CUSTOM_UNIQUE_TOKENS" -lt 1 ]]; then
        echo "ERROR: --unique-tokens must be a positive integer"; exit 1
      fi
      SEED_COUNT="$CUSTOM_UNIQUE_TOKENS"
    fi

    log "Seeding real Skyflow tokens (${SEED_COUNT} unique values)..."

    # Generate unique plaintext values
    snow_sql_silent "USE WAREHOUSE ${DATA_GEN_WH};
      CREATE OR REPLACE TABLE ${FUNC_PREFIX}.seed_plaintext AS
      SELECT
        SEQ4() AS id,
        'name_' || SEQ4()::VARCHAR AS plaintext_value
      FROM TABLE(GENERATOR(ROWCOUNT => ${SEED_COUNT}))"
    ok "Created ${SEED_COUNT} seed plaintext values"

    # Tokenize via the benchmark_tokenize function to get real Skyflow tokens
    log "  Tokenizing ${SEED_COUNT} values via Skyflow..."
    snow_sql_silent "USE WAREHOUSE ${DATA_GEN_WH};
      CREATE OR REPLACE TABLE ${FUNC_PREFIX}.seed_tokens AS
      SELECT
        id,
        plaintext_value,
        ${FUNC_PREFIX}.benchmark_tokenize(plaintext_value)::VARCHAR AS token_value
      FROM ${FUNC_PREFIX}.seed_plaintext"
    ok "Tokenized ${SEED_COUNT} values"

    # Update test tables to use real Skyflow tokens (Zipf s=1 distribution)
    # Real-world token frequency follows a power law: a small fraction of tokens
    # (popular customers, frequent products) appear in the majority of rows.
    # POW(SEED_COUNT, uniform) produces true Zipf (P(k) ∝ 1/k), giving ~15%
    # intra-batch dedup even at 500M unique tokens with 1K-row batches.
    for tbl in "${TABLES_TO_GEN[@]}"; do
      row_var="TABLE_ROWS_${tbl}"
      rows=${!row_var}
      log "  Updating ${tbl} with real tokens (Zipf distribution)..."
      snow_sql_silent "USE WAREHOUSE ${DATA_GEN_WH};
        UPDATE ${FUNC_PREFIX}.${tbl} t
        SET t.token_value = s.token_value
        FROM ${FUNC_PREFIX}.seed_tokens s
        WHERE s.id = LEAST(
          FLOOR(POW(${SEED_COUNT}, ABS(MOD(HASH(t.id), 100000000)) / 100000000.0)) - 1,
          ${SEED_COUNT} - 1)"
      ok "Updated ${tbl} with real Skyflow tokens (Zipf distribution)"
    done
  fi

  # ── Results table (with iteration and run_phase columns) ──
  snow_sql_silent "CREATE OR REPLACE TABLE ${FUNC_PREFIX}.benchmark_results (
    test_id VARCHAR DEFAULT UUID_STRING(),
    ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    warehouse_name VARCHAR,
    warehouse_size VARCHAR,
    row_count INTEGER,
    batch_config VARCHAR,
    query_id VARCHAR,
    wallclock_ms INTEGER,
    sf_elapsed_ms INTEGER,
    sf_rows_per_sec FLOAT,
    simulated_delay_ms INTEGER,
    iteration INTEGER,
    run_phase VARCHAR
  )"
  ok "Results table created"

  # ── Smoke test ──
  log "Running smoke test..."
  if $SKYFLOW_MODE; then
    # Skyflow mode: tokenize a value, then detokenize the result — verify round-trip
    log "  Skyflow smoke test: tokenize → detokenize round-trip..."
    SMOKE_TOKEN=$(snow_sql "SELECT ${FUNC_PREFIX}.benchmark_tokenize('smoke_test_value')::VARCHAR AS result" 2>&1 || true)
    if echo "$SMOKE_TOKEN" | grep -qi "error\|fail"; then
      err "Skyflow tokenize smoke test failed"
      echo "Tokenize output:"
      echo "$SMOKE_TOKEN"
      die "Smoke test failed. Check Skyflow credentials and Lambda logs."
    fi
    ok "Smoke test: tokenize succeeded"

    # Extract the token value from snowsql table output
    # Filter: remove border lines (+---+), header (RESULT), empty lines, then strip pipes/spaces
    TOKEN_VAL=$(echo "$SMOKE_TOKEN" | grep -v '^+\|^$\|RESULT\|result\|row' | grep -v '^\s*|[-]\+|' | tr -d '[:space:]|' | head -1)
    if [[ -n "$TOKEN_VAL" ]]; then
      SMOKE_DETOK=$(snow_sql "SELECT ${FUNC_PREFIX}.benchmark_detokenize('${TOKEN_VAL}')::VARCHAR AS result" 2>&1 || true)
      if echo "$SMOKE_DETOK" | grep -qi "smoke_test_value"; then
        ok "Smoke test: round-trip verified (tokenize → detokenize = original value)"
      else
        warn "Smoke test: detokenize returned unexpected result (may still work)"
        echo "  Token: ${TOKEN_VAL}"
        echo "  Detokenize output: ${SMOKE_DETOK}"
      fi
    fi

    # Also verify mock function works (skip in Skyflow mode — Lambda has no mock path when skyflowClient is set)
    if ! $SKYFLOW_MODE; then
      SMOKE_MOCK=$(snow_sql "SELECT ${FUNC_PREFIX}.benchmark_detokenize_default('test-token-123') AS result" 2>&1 || true)
      if echo "$SMOKE_MOCK" | grep -qi "DETOK_"; then
        ok "Smoke test: mock function also working"
      else
        warn "Mock function smoke test returned unexpected result"
      fi
    fi
  else
    # Mock mode smoke test
    SMOKE_RESULT=$(snow_sql "SELECT ${FUNC_PREFIX}.benchmark_detokenize_default('test-token-123') AS result" 2>&1 || true)
    if echo "$SMOKE_RESULT" | grep -qi "DETOK_"; then
      ok "Smoke test passed — external function is working"
    else
      err "Smoke test did not return expected DETOK_ prefix"
      echo "Smoke test output:"
      echo "$SMOKE_RESULT"
      echo ""
      warn "Debug tips:"
      echo "  1. Check API Gateway is deployed: curl -X POST ${API_URL}/process"
      echo "  2. Check Lambda logs: aws logs tail /aws/lambda/${LAMBDA_NAME} --region ${REGION}"
      echo "  3. Check integration: snowsql -c ${SF_CONNECTION} -q \"DESCRIBE INTEGRATION ${INTEGRATION_NAME}\""
      echo "  4. Verify IAM trust: aws iam get-role --role-name ${SF_ROLE_NAME} --region ${REGION}"
      die "Smoke test failed. Fix the issue and re-run with --skip-deploy."
    fi
  fi
fi

echo ""

###############################################################################
# Phase 4: Run Benchmarks
###############################################################################
MODE_LABEL="mock"
if $SKYFLOW_MODE; then MODE_LABEL="skyflow"; fi
log "PHASE 4: Running benchmarks (mode=${MODE_LABEL}, delay=${DELAY_MS}ms, iterations=${ITERATIONS})"

# ── Track benchmark time window for CloudWatch log fetching ──
BENCH_EARLIEST_START=""
BENCH_LATEST_END=""

# ── Probe mode: track query timestamps for CloudWatch correlation ──
if $PROBE; then
  declare -a PROBE_QUERIES=()  # "wh|tbl|rows|start_ts|end_ts|status|query_id|sf_elapsed"
fi

# ── probe_concurrency: run query to completion, poll CW throughout, then cooldown to 0 ──
# Arguments: warehouse table_name rows
# Outputs a line to stdout and appends to PROBE_QUERIES
probe_concurrency() {
  local wh="$1" tbl="$2" rows="$3"
  local func="benchmark_detokenize_default"

  local SHORT_TBL
  SHORT_TBL=$(echo "$tbl" | sed 's/test_tokens_//')

  log "Probing concurrency: ${wh} x ${SHORT_TBL} (${rows} rows)..."

  # Launch the query in background so we can poll CW while it runs
  local PROBE_SQL="
ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 600;
ALTER SESSION SET USE_CACHED_RESULT = FALSE;
USE DATABASE ${SF_DB};
USE WAREHOUSE ${wh};
SELECT SUM(LENGTH(${FUNC_PREFIX}.${func}(token_value)::VARCHAR)) FROM ${FUNC_PREFIX}.${tbl};
"
  local QUERY_START_TS
  QUERY_START_TS=$(date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || gdate -u +%Y-%m-%dT%H:%M:%SZ)

  local SNOW_OUT
  SNOW_OUT=$(mktemp)
  snowsql "${SNOWSQL_OPTS[@]}" -q "$PROBE_SQL" > "$SNOW_OUT" 2>/dev/null &
  local SNOW_PID=$!

  # ── Phase A: Poll CW while query runs, track peak ──
  local PEAK=0 POLL_NUM=0
  local POLL_INTERVAL=5

  log "  Waiting 20s for ramp-up..."
  sleep 20

  while kill -0 "$SNOW_PID" 2>/dev/null; do
    POLL_NUM=$((POLL_NUM + 1))

    local CW_NOW POLL_END POLL_START
    POLL_END=$(date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || gdate -u +%Y-%m-%dT%H:%M:%SZ)
    if [[ "$(uname)" == "Darwin" ]]; then
      POLL_START=$(python3 -c "
import datetime
dt = datetime.datetime.strptime('${POLL_END}', '%Y-%m-%dT%H:%M:%SZ')
dt = dt.replace(tzinfo=datetime.timezone.utc)
print((dt - datetime.timedelta(minutes=2)).strftime('%Y-%m-%dT%H:%M:%SZ'))
")
    else
      POLL_START=$(date -u -d "${POLL_END} - 2 minutes" +%Y-%m-%dT%H:%M:%SZ)
    fi

    CW_NOW=$(aws_ cloudwatch get-metric-statistics \
      --namespace AWS/Lambda \
      --metric-name ConcurrentExecutions \
      --dimensions Name=FunctionName,Value="${LAMBDA_NAME}" \
      --start-time "$POLL_START" \
      --end-time "$POLL_END" \
      --period 60 \
      --statistics Maximum \
      --query 'max_by(Datapoints, &Maximum).Maximum' \
      --output text 2>/dev/null || echo "0")

    if [[ "$CW_NOW" == "None" || -z "$CW_NOW" ]]; then
      CW_NOW=0
    else
      CW_NOW=$(printf "%.0f" "$CW_NOW")
    fi

    if [[ $CW_NOW -gt $PEAK ]]; then
      PEAK=$CW_NOW
    fi

    printf "  Poll %2d: CW_concurrent=%d, peak=%d (query running)\n" \
      "$POLL_NUM" "$CW_NOW" "$PEAK"

    sleep "$POLL_INTERVAL"
  done

  wait "$SNOW_PID" 2>/dev/null || true

  local QUERY_END_TS
  QUERY_END_TS=$(date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || gdate -u +%Y-%m-%dT%H:%M:%SZ)

  local ELAPSED_MS
  if [[ "$(uname)" == "Darwin" ]]; then
    ELAPSED_MS=$(python3 -c "
import datetime
s = datetime.datetime.strptime('${QUERY_START_TS}', '%Y-%m-%dT%H:%M:%SZ')
e = datetime.datetime.strptime('${QUERY_END_TS}', '%Y-%m-%dT%H:%M:%SZ')
print(int((e - s).total_seconds() * 1000))
")
  else
    ELAPSED_MS=$(( $(date -d "$QUERY_END_TS" +%s) * 1000 - $(date -d "$QUERY_START_TS" +%s) * 1000 ))
  fi

  log "  Query completed in ${ELAPSED_MS}ms (peak=${PEAK})"

  # ── Phase B: Cooldown — poll CW until concurrent executions reach 0 ──
  log "  Cooldown: waiting for CW concurrent executions to reach 0..."
  local ZERO_COUNT=0 ZERO_THRESHOLD=3 CD_POLLS=0 CD_MAX=120  # 120*5s = 10 min max

  while [[ $CD_POLLS -lt $CD_MAX ]]; do
    CD_POLLS=$((CD_POLLS + 1))
    sleep "$POLL_INTERVAL"

    local CD_END CD_START CD_VAL
    CD_END=$(date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || gdate -u +%Y-%m-%dT%H:%M:%SZ)
    if [[ "$(uname)" == "Darwin" ]]; then
      CD_START=$(python3 -c "
import datetime
dt = datetime.datetime.strptime('${CD_END}', '%Y-%m-%dT%H:%M:%SZ')
dt = dt.replace(tzinfo=datetime.timezone.utc)
print((dt - datetime.timedelta(minutes=2)).strftime('%Y-%m-%dT%H:%M:%SZ'))
")
    else
      CD_START=$(date -u -d "${CD_END} - 2 minutes" +%Y-%m-%dT%H:%M:%SZ)
    fi

    CD_VAL=$(aws_ cloudwatch get-metric-statistics \
      --namespace AWS/Lambda \
      --metric-name ConcurrentExecutions \
      --dimensions Name=FunctionName,Value="${LAMBDA_NAME}" \
      --start-time "$CD_START" \
      --end-time "$CD_END" \
      --period 60 \
      --statistics Maximum \
      --query 'max_by(Datapoints, &Timestamp).Maximum' \
      --output text 2>/dev/null || echo "0")

    if [[ "$CD_VAL" == "None" || -z "$CD_VAL" ]]; then
      CD_VAL=0
    else
      CD_VAL=$(printf "%.0f" "$CD_VAL")
    fi

    if [[ $CD_VAL -le 0 ]]; then
      ZERO_COUNT=$((ZERO_COUNT + 1))
    else
      ZERO_COUNT=0
    fi

    printf "  Cooldown %2d: CW_concurrent=%d, zero_streak=%d/%d\n" \
      "$CD_POLLS" "$CD_VAL" "$ZERO_COUNT" "$ZERO_THRESHOLD"

    if [[ $ZERO_COUNT -ge $ZERO_THRESHOLD ]]; then
      break
    fi
  done

  if [[ $ZERO_COUNT -ge $ZERO_THRESHOLD ]]; then
    ok "Cooldown complete — CW at 0 for ${ZERO_THRESHOLD} consecutive polls"
  else
    warn "Cooldown timed out after $((CD_POLLS * POLL_INTERVAL))s (CW still >0)"
  fi

  # Record results
  PROBE_QUERIES+=("${wh}|${tbl}|${rows}|${QUERY_START_TS}|${QUERY_END_TS}|complete|probe|${ELAPSED_MS}|${PEAK}")

  printf "%-10s | %-14s | %-8s | %10d | %8dms\n" \
    "$wh" "$SHORT_TBL" "complete" "$PEAK" "$ELAPSED_MS"

  rm -f "$SNOW_OUT"
}

# ── run_one_benchmark: runs a single benchmark query and records the result ──
# Arguments: warehouse table_name rows iteration run_phase
run_one_benchmark() {
  local wh="$1" tbl="$2" rows="$3" iter="$4" run_phase="$5"
  local size func

  size="$(wh_size "$wh")"

  # Probe mode always uses mock function
  if $PROBE; then
    func="benchmark_detokenize_default"
  elif $SKYFLOW_MODE; then
    func="benchmark_detokenize"
  else
    func="benchmark_detokenize_default"
  fi

  # ── Single snowsql session: benchmark + metrics ──
  # All statements run in ONE session so LAST_QUERY_ID() correctly
  # references the benchmark query.
  local TIMEOUT_SQL=""
  if $PROBE; then
    TIMEOUT_SQL="ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 600;"
  fi

  BENCH_SQL="
${TIMEOUT_SQL}
ALTER SESSION SET USE_CACHED_RESULT = FALSE;
USE DATABASE ${SF_DB};
USE WAREHOUSE ${wh};
SELECT COUNT(*) FROM ${FUNC_PREFIX}.${tbl};
SELECT SUM(LENGTH(${FUNC_PREFIX}.${func}(token_value)::VARCHAR)) FROM ${FUNC_PREFIX}.${tbl};
SELECT
  LAST_QUERY_ID() AS QUERY_ID,
  q.TOTAL_ELAPSED_TIME AS SF_ELAPSED_MS
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(RESULT_LIMIT => 10)) q
WHERE q.QUERY_ID = LAST_QUERY_ID()
LIMIT 1;
"

  # Record start timestamp (UTC) for CloudWatch correlation
  local QUERY_START_TS QUERY_END_TS
  QUERY_START_TS=$(date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || gdate -u +%Y-%m-%dT%H:%M:%SZ)

  local START_NS BENCH_OUTPUT EXIT_CODE WALLCLOCK_MS METRICS_JSON
  START_NS=$(nanos)
  set +e
  BENCH_OUTPUT=$(snowsql "${SNOWSQL_OPTS[@]}" -q "$BENCH_SQL" -o output_format=json 2>/dev/null)
  EXIT_CODE=$?
  set -e
  WALLCLOCK_MS=$(millis_since "$START_NS")

  QUERY_END_TS=$(date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || gdate -u +%Y-%m-%dT%H:%M:%SZ)

  # Update benchmark time window for Phase 5 CloudWatch log fetching
  if [[ -z "$BENCH_EARLIEST_START" ]] || [[ "$QUERY_START_TS" < "$BENCH_EARLIEST_START" ]]; then
    BENCH_EARLIEST_START="$QUERY_START_TS"
  fi
  if [[ -z "$BENCH_LATEST_END" ]] || [[ "$QUERY_END_TS" > "$BENCH_LATEST_END" ]]; then
    BENCH_LATEST_END="$QUERY_END_TS"
  fi

  # Detect timeout: snowsql error + wall time near 180s
  local STATUS="complete"
  if [[ $EXIT_CODE -ne 0 ]]; then
    if [[ $WALLCLOCK_MS -gt 590000 ]]; then
      STATUS="timeout"
    else
      STATUS="error"
    fi
  fi

  # Last JSON array = metrics query result
  METRICS_JSON=$(echo "$BENCH_OUTPUT" | grep '^\[' | tail -1)

  # Parse Snowflake-side metrics (QUERY_ID + SF_ELAPSED_MS)
  local QUERY_ID SF_ELAPSED
  set +e
  QUERY_ID=$(echo "$METRICS_JSON" | jq -r '.[0].QUERY_ID // "unknown"' 2>/dev/null)
  SF_ELAPSED=$(echo "$METRICS_JSON" | jq -r '.[0].SF_ELAPSED_MS // 0' 2>/dev/null)
  set -e
  QUERY_ID=${QUERY_ID:-unknown}; QUERY_ID=${QUERY_ID//null/unknown}
  SF_ELAPSED=${SF_ELAPSED:-0}; SF_ELAPSED=${SF_ELAPSED//null/0}

  # On timeout, use wallclock as SF elapsed estimate
  if [[ "$STATUS" == "timeout" && "$SF_ELAPSED" == "0" ]]; then
    SF_ELAPSED=$WALLCLOCK_MS
  fi

  # Derived metrics
  local SF_ROWS_PER_SEC=0
  if [[ "$SF_ELAPSED" -gt 0 ]]; then
    SF_ROWS_PER_SEC=$(( rows * 1000 / SF_ELAPSED ))
  fi

  # Store results
  snow_sql_silent "INSERT INTO ${FUNC_PREFIX}.benchmark_results
    (warehouse_name, warehouse_size, row_count, batch_config, query_id,
     wallclock_ms, sf_elapsed_ms,
     sf_rows_per_sec, simulated_delay_ms, iteration, run_phase)
    VALUES ('${wh}', '${size}', ${rows}, 'default', '${QUERY_ID}',
     ${WALLCLOCK_MS}, ${SF_ELAPSED},
     ${SF_ROWS_PER_SEC}, ${DELAY_MS}, ${iter}, '${run_phase}')"

  # Record probe query data for CloudWatch analysis
  if $PROBE; then
    PROBE_QUERIES+=("${wh}|${tbl}|${rows}|${QUERY_START_TS}|${QUERY_END_TS}|${STATUS}|${QUERY_ID}|${SF_ELAPSED}")
  fi

  # Print progress
  local STATUS_SUFFIX=""
  if [[ "$STATUS" != "complete" ]]; then
    STATUS_SUFFIX=" [${STATUS}]"
  fi
  printf "%-8s | %-18s | %4d/%-4d | %6dms | %6dms | %10d | %s%s\n" \
    "$wh" "$tbl" "$iter" "$ITERATIONS" "$WALLCLOCK_MS" "$SF_ELAPSED" "$SF_ROWS_PER_SEC" "$run_phase" "$STATUS_SUFFIX"
}

# Build test matrix
declare -a MATRIX=()
for wh in "${ALL_WAREHOUSES[@]}"; do
  for tbl in "${ALL_TABLES[@]}"; do
    MATRIX+=("${wh}|${tbl}")
  done
done

TOTAL_COMBOS=${#MATRIX[@]}
TOTAL_QUERIES=$(( TOTAL_COMBOS * ITERATIONS ))
if $PROBE; then
  TOTAL_WITH_WARMUP=$TOTAL_QUERIES
  log "Test matrix: ${TOTAL_COMBOS} combos x ${ITERATIONS} iteration = ${TOTAL_QUERIES} queries (no warmup, 180s timeout)"
else
  TOTAL_WITH_WARMUP=$(( TOTAL_COMBOS + TOTAL_QUERIES ))
  log "Test matrix: ${TOTAL_COMBOS} combos x ${ITERATIONS} iterations = ${TOTAL_QUERIES} measured + ${TOTAL_COMBOS} warmups"
fi
echo ""

if $PROBE; then
  # Probe mode: use concurrency polling for large tables (>=10M rows)
  # Small tables still use run_one_benchmark for batch size confirmation
  printf "%-10s | %-14s | %-8s | %10s | %8s\n" \
    "WH" "TABLE" "STATUS" "CW_PEAK" "ELAPSED"
  printf "%s\n" \
    "-----------|----------------|----------|------------|--------"

  for entry in "${MATRIX[@]}"; do
    IFS='|' read -r wh tbl <<< "$entry"
    row_var="TABLE_ROWS_${tbl}"
    rows=${!row_var}

    if [[ $rows -ge 10000000 ]]; then
      # Large table: run to completion with CW polling + full cooldown to 0
      probe_concurrency "$wh" "$tbl" "$rows"
    else
      # Small table: run normally for batch size data
      run_one_benchmark "$wh" "$tbl" "$rows" 1 "measured"
    fi
  done
else
  printf "%-8s | %-18s | %-9s | %8s | %8s | %10s | %s\n" \
    "WH" "TABLE" "ITER" "WALL_MS" "SF_MS" "SF_RPS" "PHASE"
  printf "%s\n" \
    "---------|--------------------|-----------|---------|---------|------------|----------"

  for entry in "${MATRIX[@]}"; do
    IFS='|' read -r wh tbl <<< "$entry"

    row_var="TABLE_ROWS_${tbl}"
    rows=${!row_var}

    # ── Warmup pass (sacrificial, not recorded in measured results) ──
    log "Warmup: ${wh} x ${tbl}..."
    run_one_benchmark "$wh" "$tbl" "$rows" 0 "warmup"

    # ── Measured iterations ──
    for iter in $(seq 1 "$ITERATIONS"); do
      run_one_benchmark "$wh" "$tbl" "$rows" "$iter" "measured"
    done
  done
fi

echo ""

###############################################################################
# Phase 4b: 10B Validation (optional)
###############################################################################
if $VALIDATE_10B; then
  log "PHASE 4b: 10B validation (XL/2XL x 10B x 1 iteration)"
  echo ""

  VALIDATE_WAREHOUSES=(BENCH_XL BENCH_2XL)
  VALIDATE_TABLE="test_tokens_10b"
  VALIDATE_ROWS=${TABLE_ROWS_test_tokens_10b}

  for wh in "${VALIDATE_WAREHOUSES[@]}"; do
    # Warmup
    log "Warmup: ${wh} x ${VALIDATE_TABLE}..."
    run_one_benchmark "$wh" "$VALIDATE_TABLE" "$VALIDATE_ROWS" 0 "warmup"

    # Single measured iteration
    run_one_benchmark "$wh" "$VALIDATE_TABLE" "$VALIDATE_ROWS" 1 "validate_10b"
  done

  echo ""
fi

###############################################################################
# Phase 5: Display Results
###############################################################################
log "PHASE 5: Results Summary"
echo ""

# ── Full results table (measured runs only) ──
log "All Measured Results:"
snow_sql "SELECT
    warehouse_name AS wh,
    row_count AS num_rows,
    iteration AS iter,
    sf_elapsed_ms AS sf_ms,
    wallclock_ms AS wall_ms,
    ROUND(sf_rows_per_sec, 0) AS sf_rps,
    run_phase AS phase,
    query_id
  FROM ${FUNC_PREFIX}.benchmark_results
  WHERE run_phase <> 'warmup' AND simulated_delay_ms = ${DELAY_MS}
  ORDER BY warehouse_size, row_count, iteration"
echo ""

# ── Statistical summary: median / min / max / spread% per warehouse × table ──
log "Statistical Summary (median / min / max / spread% across ${ITERATIONS} iterations):"
snow_sql "SELECT
    warehouse_name AS wh,
    row_count AS num_rows,
    COUNT(*) AS iters,
    ROUND(MEDIAN(sf_rows_per_sec), 0) AS median_rps,
    ROUND(MIN(sf_rows_per_sec), 0) AS min_rps,
    ROUND(MAX(sf_rows_per_sec), 0) AS max_rps,
    ROUND(
      CASE WHEN MEDIAN(sf_rows_per_sec) > 0
        THEN (MAX(sf_rows_per_sec) - MIN(sf_rows_per_sec)) / MEDIAN(sf_rows_per_sec) * 100
        ELSE 0
      END, 1
    ) AS spread_pct,
    ROUND(MEDIAN(sf_elapsed_ms), 0) AS median_sf_ms,
    ROUND(MEDIAN(wallclock_ms), 0) AS median_wall_ms
  FROM ${FUNC_PREFIX}.benchmark_results
  WHERE run_phase = 'measured' AND simulated_delay_ms = ${DELAY_MS}
  GROUP BY warehouse_name, warehouse_size, row_count
  ORDER BY warehouse_size, row_count"
echo ""

# ── Pivot: median SF rows/sec by warehouse × table ──
log "Pivot: Median SF Rows/sec by Warehouse (delay=${DELAY_MS}ms):"
snow_sql "WITH stats AS (
    SELECT
      warehouse_name,
      warehouse_size,
      row_count,
      MEDIAN(sf_rows_per_sec) AS median_rps
    FROM ${FUNC_PREFIX}.benchmark_results
    WHERE run_phase = 'measured' AND simulated_delay_ms = ${DELAY_MS}
    GROUP BY warehouse_name, warehouse_size, row_count
  )
  SELECT
    row_count AS num_rows,
    COALESCE(TO_VARCHAR(MAX(CASE WHEN warehouse_name = 'BENCH_XS' THEN ROUND(median_rps, 0) END)), '-') AS XS,
    COALESCE(TO_VARCHAR(MAX(CASE WHEN warehouse_name = 'BENCH_M' THEN ROUND(median_rps, 0) END)), '-') AS M,
    COALESCE(TO_VARCHAR(MAX(CASE WHEN warehouse_name = 'BENCH_XL' THEN ROUND(median_rps, 0) END)), '-') AS XL,
    COALESCE(TO_VARCHAR(MAX(CASE WHEN warehouse_name = 'BENCH_2XL' THEN ROUND(median_rps, 0) END)), '-') AS XXL,
    COALESCE(TO_VARCHAR(MAX(CASE WHEN warehouse_name = 'BENCH_3XL' THEN ROUND(median_rps, 0) END)), '-') AS XXXL,
    COALESCE(TO_VARCHAR(MAX(CASE WHEN warehouse_name = 'BENCH_4XL' THEN ROUND(median_rps, 0) END)), '-') AS XXXXL
  FROM stats
  GROUP BY row_count
  ORDER BY row_count"
echo ""

# ── 10B validation results (if applicable) ──
if $VALIDATE_10B; then
  log "10B Validation Results:"
  snow_sql "SELECT
      warehouse_name AS wh,
      row_count AS num_rows,
      sf_elapsed_ms AS sf_ms,
      wallclock_ms AS wall_ms,
      ROUND(sf_rows_per_sec, 0) AS sf_rps,
      query_id
    FROM ${FUNC_PREFIX}.benchmark_results
    WHERE run_phase = 'validate_10b' AND simulated_delay_ms = ${DELAY_MS}
    ORDER BY warehouse_size"
  echo ""
fi

# ── Probe mode: CloudWatch METRIC log analysis ──
if $PROBE && [[ ${#PROBE_QUERIES[@]} -gt 0 ]]; then
  LOG_GROUP="/aws/lambda/${LAMBDA_NAME}"

  # Collect all METRIC log lines across all probe queries
  # Use the earliest start and latest end as the time window
  PROBE_LOG_FILE=$(mktemp)
  trap "rm -f '$PROBE_LOG_FILE'" EXIT

  # Wait for CloudWatch data to become available (Lambda metrics have ~1-2 min delay)
  log "Waiting 60s for CloudWatch metrics to propagate..."
  sleep 60
  ok "CloudWatch wait complete"

  log "Fetching CloudWatch data for ${#PROBE_QUERIES[@]} probe queries..."

  # Find the overall time window (add 2 min buffer on each side)
  EARLIEST_START=""
  LATEST_END=""
  for pq in "${PROBE_QUERIES[@]}"; do
    IFS='|' read -r _wh _tbl _rows pq_start pq_end _status _qid _sf_elapsed <<< "$pq"
    if [[ -z "$EARLIEST_START" ]] || [[ "$pq_start" < "$EARLIEST_START" ]]; then
      EARLIEST_START="$pq_start"
    fi
    if [[ -z "$LATEST_END" ]] || [[ "$pq_end" > "$LATEST_END" ]]; then
      LATEST_END="$pq_end"
    fi
  done

  # Convert to epoch ms for CloudWatch log filter (add 2 min buffer)
  if [[ "$(uname)" == "Darwin" ]]; then
    CW_START_MS=$(python3 -c "
import datetime
dt = datetime.datetime.strptime('${EARLIEST_START}', '%Y-%m-%dT%H:%M:%SZ')
dt = dt.replace(tzinfo=datetime.timezone.utc)
print(int((dt.timestamp() - 120) * 1000))
")
    CW_END_MS=$(python3 -c "
import datetime
dt = datetime.datetime.strptime('${LATEST_END}', '%Y-%m-%dT%H:%M:%SZ')
dt = dt.replace(tzinfo=datetime.timezone.utc)
print(int((dt.timestamp() + 120) * 1000))
")
  else
    CW_START_MS=$(( $(date -d "$EARLIEST_START" +%s) * 1000 - 120000 ))
    CW_END_MS=$(( $(date -d "$LATEST_END" +%s) * 1000 + 120000 ))
  fi

  # Fetch all METRIC lines from CloudWatch Logs in one call
  PROBE_LOG_FILE=$(mktemp)
  trap "rm -f '$PROBE_LOG_FILE'" EXIT
  aws_ logs filter-log-events \
    --log-group-name "$LOG_GROUP" \
    --start-time "$CW_START_MS" \
    --end-time "$CW_END_MS" \
    --filter-pattern "METRIC" \
    --query 'events[].message' \
    --output text > "$PROBE_LOG_FILE" 2>/dev/null || warn "Could not fetch CloudWatch logs"

  METRIC_LINE_COUNT=$(wc -l < "$PROBE_LOG_FILE" | tr -d ' ')
  ok "Fetched ${METRIC_LINE_COUNT} METRIC log lines"

  # ── Parse METRIC logs per query and build pipeline analysis ──
  echo ""
  log "=== Snowflake → Lambda Pipeline Analysis ==="
  echo ""
  printf "%-10s | %-14s | %-8s | %11s | %9s | %9s | %9s | %10s | %10s | %10s | %8s\n" \
    "WH" "TABLE" "STATUS" "INVOCATIONS" "BATCH_MIN" "BATCH_P50" "BATCH_MAX" "LOG_INSTS" "CW_PEAK" "THROUGHPUT" "SF_MS"
  printf "%s\n" \
    "-----------|----------------|----------|-------------|-----------|-----------|-----------|------------|------------|------------|--------"

  for pq in "${PROBE_QUERIES[@]}"; do
    # Parse fields — probe_concurrency entries have 9 fields (includes PEAK),
    # run_one_benchmark entries have 8 fields
    pq_wh=""; pq_tbl=""; pq_rows=""; pq_start=""; pq_end=""; pq_status=""; pq_qid=""; pq_sf_elapsed=""; pq_peak=""
    IFS='|' read -r pq_wh pq_tbl pq_rows pq_start pq_end pq_status pq_qid pq_sf_elapsed pq_peak <<< "$pq"

    # Filter METRIC lines for this query_id
    if [[ "$pq_qid" != "unknown" && "$pq_qid" != "probe" ]]; then
      QUERY_METRICS=$(grep "query_id=${pq_qid}" "$PROBE_LOG_FILE" 2>/dev/null || true)
    else
      QUERY_METRICS=""
    fi

    INVOCATIONS=0
    BATCH_MIN=0
    BATCH_P50=0
    BATCH_MAX=0
    LOG_INSTANCES=0
    THROUGHPUT=0

    if [[ -n "$QUERY_METRICS" ]]; then
      BATCH_SIZES=$(echo "$QUERY_METRICS" | grep -oE 'batch_size=[0-9]+' | cut -d= -f2 | sort -n)
      INVOCATIONS=$(echo "$BATCH_SIZES" | wc -l | tr -d ' ')

      if [[ $INVOCATIONS -gt 0 ]]; then
        BATCH_MIN=$(echo "$BATCH_SIZES" | head -1)
        BATCH_MAX=$(echo "$BATCH_SIZES" | tail -1)
        P50_IDX=$(( (INVOCATIONS + 1) / 2 ))
        BATCH_P50=$(echo "$BATCH_SIZES" | sed -n "${P50_IDX}p")
        LOG_INSTANCES=$(echo "$QUERY_METRICS" | grep -oE 'instance=[^ ]+' | sort -u | wc -l | tr -d ' ')
      fi
    fi

    # Throughput = rows / sf_elapsed_seconds
    if [[ "$pq_sf_elapsed" -gt 0 ]]; then
      THROUGHPUT=$(( pq_rows * 1000 / pq_sf_elapsed ))
    fi

    # CW_PEAK: use the live-polled peak if available (from probe_concurrency),
    # otherwise fall back to post-hoc CloudWatch fetch
    CW_PEAK="-"
    if [[ -n "$pq_peak" && "$pq_peak" -gt 0 ]] 2>/dev/null; then
      CW_PEAK="$pq_peak"
    else
      # Post-hoc fetch for run_one_benchmark entries
      if [[ "$(uname)" == "Darwin" ]]; then
        PADDED_START=$(python3 -c "
import datetime
dt = datetime.datetime.strptime('${pq_start}', '%Y-%m-%dT%H:%M:%SZ')
dt = dt.replace(tzinfo=datetime.timezone.utc)
print((dt - datetime.timedelta(minutes=1)).strftime('%Y-%m-%dT%H:%M:%SZ'))
")
        PADDED_END=$(python3 -c "
import datetime
dt = datetime.datetime.strptime('${pq_end}', '%Y-%m-%dT%H:%M:%SZ')
dt = dt.replace(tzinfo=datetime.timezone.utc)
print((dt + datetime.timedelta(minutes=2)).strftime('%Y-%m-%dT%H:%M:%SZ'))
")
      else
        PADDED_START=$(date -u -d "${pq_start} - 1 minute" +%Y-%m-%dT%H:%M:%SZ)
        PADDED_END=$(date -u -d "${pq_end} + 2 minutes" +%Y-%m-%dT%H:%M:%SZ)
      fi

      CW_RAW=$(aws_ cloudwatch get-metric-statistics \
        --namespace AWS/Lambda \
        --metric-name ConcurrentExecutions \
        --dimensions Name=FunctionName,Value="${LAMBDA_NAME}" \
        --start-time "$PADDED_START" \
        --end-time "$PADDED_END" \
        --period 60 \
        --statistics Maximum \
        --query 'max_by(Datapoints, &Maximum).Maximum' \
        --output text 2>/dev/null || echo "")
      if [[ -n "$CW_RAW" && "$CW_RAW" != "None" && "$CW_RAW" != "null" ]]; then
        CW_PEAK=$(printf "%.0f" "$CW_RAW")
      fi
    fi

    SHORT_TBL=$(echo "$pq_tbl" | sed 's/test_tokens_//')

    printf "%-10s | %-14s | %-8s | %11d | %9d | %9d | %9d | %10d | %10s | %10d | %8s\n" \
      "$pq_wh" "$SHORT_TBL" "$pq_status" "$INVOCATIONS" "$BATCH_MIN" "$BATCH_P50" "$BATCH_MAX" "$LOG_INSTANCES" "$CW_PEAK" "$THROUGHPUT" "$pq_sf_elapsed"
  done
  echo ""
  echo "  LOG_INSTS  = unique Lambda instances seen in CloudWatch Logs (total over query lifetime)"
  echo "  CW_PEAK    = peak ConcurrentExecutions from CloudWatch Metrics (live-polled for large tables)"
  echo "  THROUGHPUT = rows / sf_elapsed_seconds (tokens/sec)"
  echo ""

  rm -f "$PROBE_LOG_FILE"
fi

# ── Batch Token Dedup Analysis (all modes) ──
if [[ -n "$BENCH_EARLIEST_START" && -n "$BENCH_LATEST_END" ]]; then
  LOG_GROUP_DEDUP="/aws/lambda/${LAMBDA_NAME}"

  log "Waiting 15s for CloudWatch log propagation..."
  sleep 15

  # Convert time window to epoch ms (add 2 min buffer each side)
  if [[ "$(uname)" == "Darwin" ]]; then
    DEDUP_CW_START_MS=$(python3 -c "
import datetime
dt = datetime.datetime.strptime('${BENCH_EARLIEST_START}', '%Y-%m-%dT%H:%M:%SZ')
dt = dt.replace(tzinfo=datetime.timezone.utc)
print(int((dt.timestamp() - 120) * 1000))
")
    DEDUP_CW_END_MS=$(python3 -c "
import datetime
dt = datetime.datetime.strptime('${BENCH_LATEST_END}', '%Y-%m-%dT%H:%M:%SZ')
dt = dt.replace(tzinfo=datetime.timezone.utc)
print(int((dt.timestamp() + 120) * 1000))
")
  else
    DEDUP_CW_START_MS=$(( $(date -d "$BENCH_EARLIEST_START" +%s) * 1000 - 120000 ))
    DEDUP_CW_END_MS=$(( $(date -d "$BENCH_LATEST_END" +%s) * 1000 + 120000 ))
  fi

  DEDUP_LOG_FILE=$(mktemp)
  aws_ logs filter-log-events \
    --log-group-name "$LOG_GROUP_DEDUP" \
    --start-time "$DEDUP_CW_START_MS" \
    --end-time "$DEDUP_CW_END_MS" \
    --filter-pattern "METRIC" \
    --query 'events[].message' \
    --output text > "$DEDUP_LOG_FILE" 2>/dev/null || warn "Could not fetch CloudWatch logs for dedup analysis"

  DEDUP_LINE_COUNT=$(wc -l < "$DEDUP_LOG_FILE" | tr -d ' ')

  if [[ "$DEDUP_LINE_COUNT" -gt 0 ]]; then
    # Parse unique_tokens, batch_size, and skyflow_wall_ms from each METRIC line (portable awk — no gawk extensions)
    DEDUP_STATS=$(awk '
      /unique_tokens=[0-9]/ {
        bs = 0; ut = 0; sw = 0
        n_fields = split($0, fields, " ")
        for (i = 1; i <= n_fields; i++) {
          if (fields[i] ~ /^batch_size=/) { split(fields[i], kv, "="); bs = kv[2]+0 }
          if (fields[i] ~ /^unique_tokens=/) { split(fields[i], kv, "="); ut = kv[2]+0 }
          if (fields[i] ~ /^skyflow_wall_ms=/) { split(fields[i], kv, "="); sw = kv[2]+0 }
        }
        if (bs > 0) {
          n++
          sum_bs += bs
          sum_ut += ut
          sum_rep += (bs - ut)
          sum_dedup += (1 - ut/bs) * 100
          if (sw > 0) { n_sw++; sum_sf_tps += bs * 1000 / sw }
        }
      }
      END {
        if (n > 0) {
          sf_tps = (n_sw > 0) ? sum_sf_tps / n_sw : 0
          printf "%d %.1f %.1f %.1f %.1f %.0f\n", n, sum_bs/n, sum_ut/n, sum_rep/n, sum_dedup/n, sf_tps
        } else {
          printf "0 0 0 0 0 0\n"
        }
      }
    ' "$DEDUP_LOG_FILE")

    DEDUP_N=$(echo "$DEDUP_STATS" | awk '{print $1}')
    DEDUP_AVG_BS=$(echo "$DEDUP_STATS" | awk '{print $2}')
    DEDUP_AVG_UT=$(echo "$DEDUP_STATS" | awk '{print $3}')
    DEDUP_AVG_REP=$(echo "$DEDUP_STATS" | awk '{print $4}')
    DEDUP_AVG_PCT=$(echo "$DEDUP_STATS" | awk '{print $5}')
    DEDUP_AVG_SF_TPS=$(echo "$DEDUP_STATS" | awk '{print $6}')

    if [[ "$DEDUP_N" -gt 0 ]]; then
      printf "  Batch dedup (%s samples): avg_batch=%s  unique=%s  repeated=%s  dedup=%s%%\n" \
        "$DEDUP_N" "$DEDUP_AVG_BS" "$DEDUP_AVG_UT" "$DEDUP_AVG_REP" "$DEDUP_AVG_PCT"
      echo ""
    else
      warn "No METRIC lines with unique_tokens found in CloudWatch logs"
      echo ""
    fi
  else
    warn "No METRIC log lines found in CloudWatch (window: ${BENCH_EARLIEST_START} to ${BENCH_LATEST_END})"
    echo ""
  fi

  rm -f "$DEDUP_LOG_FILE"
fi

# ── CloudWatch metrics (best-effort, 60-minute window) ──
log "CloudWatch Lambda Metrics (last 60 minutes):"
END_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || gdate -u +%Y-%m-%dT%H:%M:%SZ)
START_TIME=$(date -u -v-60M +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || gdate -u -d '60 minutes ago' +%Y-%m-%dT%H:%M:%SZ)

# Fetch all three metrics as JSON
CW_CONCURRENT=$(aws_ cloudwatch get-metric-statistics \
  --namespace AWS/Lambda --metric-name ConcurrentExecutions \
  --dimensions Name=FunctionName,Value="${LAMBDA_NAME}" \
  --start-time "$START_TIME" --end-time "$END_TIME" \
  --period 60 --statistics Maximum --output json 2>/dev/null || echo '{"Datapoints":[]}')

CW_INVOCATIONS=$(aws_ cloudwatch get-metric-statistics \
  --namespace AWS/Lambda --metric-name Invocations \
  --dimensions Name=FunctionName,Value="${LAMBDA_NAME}" \
  --start-time "$START_TIME" --end-time "$END_TIME" \
  --period 60 --statistics Sum --output json 2>/dev/null || echo '{"Datapoints":[]}')

CW_THROTTLES=$(aws_ cloudwatch get-metric-statistics \
  --namespace AWS/Lambda --metric-name Throttles \
  --dimensions Name=FunctionName,Value="${LAMBDA_NAME}" \
  --start-time "$START_TIME" --end-time "$END_TIME" \
  --period 60 --statistics Sum --output json 2>/dev/null || echo '{"Datapoints":[]}')

# Merge and format into a compact table
CW_TABLE=$(jq -r -n \
  --argjson c "$CW_CONCURRENT" \
  --argjson i "$CW_INVOCATIONS" \
  --argjson t "$CW_THROTTLES" '
  # Collect all timestamps
  ([$c.Datapoints[].Timestamp, $i.Datapoints[].Timestamp, $t.Datapoints[].Timestamp] | unique) as $times |
  # Build lookup maps
  ($c.Datapoints | map({(.Timestamp): .Maximum}) | add // {}) as $cmap |
  ($i.Datapoints | map({(.Timestamp): .Sum}) | add // {}) as $imap |
  ($t.Datapoints | map({(.Timestamp): .Sum}) | add // {}) as $tmap |
  # Sort and output last 10 rows
  ($times | sort | .[-10:])[] |
  . as $ts |
  ($ts | split("T")[1] | split(":")[0:2] | join(":")) as $hm |
  "\($hm)\t\($cmap[$ts] // "-" | tostring | split(".")[0])\t\($imap[$ts] // "-" | tostring | split(".")[0])\t\($tmap[$ts] // "-" | tostring | split(".")[0])"
' 2>/dev/null)

if [[ -n "$CW_TABLE" ]]; then
  printf "  %-18s| %10s | %12s | %9s\n" "TIME (UTC)" "CONCURRENT" "INVOCATIONS" "THROTTLES"
  printf "  %-18s|%s|%s|%s\n" "------------------" "------------" "--------------" "-----------"
  while IFS=$'\t' read -r hm conc inv thr; do
    printf "  %-18s| %10s | %12s | %9s\n" "$hm" "$conc" "$inv" "$thr"
  done <<< "$CW_TABLE"
else
  warn "No CloudWatch metric data available"
fi

# ── Highlights ──
CW_PEAK_CONC=$(jq '[.Datapoints[].Maximum] | max // 0 | floor' <<< "$CW_CONCURRENT" 2>/dev/null || echo "-")
[[ "$CW_PEAK_CONC" == "0" || -z "$CW_PEAK_CONC" ]] && CW_PEAK_CONC="-"

# Query median throughput, rows, and sf elapsed from Snowflake
# Aliases avoid reserved words (ROWS) and digits in column headers so parsing is safe
HL_SF_QUERY=$(snow_sql "SELECT ROUND(MEDIAN(sf_rows_per_sec), 0) AS rps, MAX(row_count) AS vol, ROUND(MEDIAN(sf_elapsed_ms), 0) AS sfms FROM ${FUNC_PREFIX}.benchmark_results WHERE run_phase = 'measured' AND simulated_delay_ms = ${DELAY_MS}")
# Extract first data row: starts with | and contains a digit (skips header + separator lines)
HL_DATA_LINE=$(echo "$HL_SF_QUERY" | awk '/^[|]/ && /[0-9]/' | head -1)
HIGHLIGHT_RPS=$(echo "$HL_DATA_LINE" | awk -F'|' '{gsub(/[ ]+/,"",$2); print $2}')
HL_SF_ROWS=$(echo "$HL_DATA_LINE" | awk -F'|' '{gsub(/[ ]+/,"",$3); print $3}')
HL_SF_MS=$(echo "$HL_DATA_LINE" | awk -F'|' '{gsub(/[ ]+/,"",$4); print $4}')
[[ -z "$HIGHLIGHT_RPS" || "$HIGHLIGHT_RPS" == "NULL" ]] && HIGHLIGHT_RPS="-"
[[ -z "$HL_SF_ROWS" || "$HL_SF_ROWS" == "NULL" ]] && HL_SF_ROWS="-"
[[ -z "$HL_SF_MS" || "$HL_SF_MS" == "NULL" ]] && HL_SF_MS="-"

# Snowflake avg batch size (rounded to int)
HL_SF_BATCH=$(printf "%.0f" "${DEDUP_AVG_BS:-0}" 2>/dev/null || echo "-")
[[ "$HL_SF_BATCH" == "0" || -z "$HL_SF_BATCH" ]] && HL_SF_BATCH="-"

# Skyflow: throughput, total tokens, and dedup from METRIC logs
HL_SF_TPS="${DEDUP_AVG_SF_TPS:-0}"
[[ "$HL_SF_TPS" == "0" || -z "$HL_SF_TPS" ]] && HL_SF_TPS="-"
# Total tokens = invocations * avg batch size
if [[ "${DEDUP_N:-0}" -gt 0 && "${DEDUP_AVG_BS:-0}" != "0" ]]; then
  HL_TOTAL_TOKENS=$(awk "BEGIN { printf \"%.0f\", ${DEDUP_N} * ${DEDUP_AVG_BS} }")
else
  HL_TOTAL_TOKENS="-"
fi
HL_DEDUP_PCT="${DEDUP_AVG_PCT:-"-"}"
[[ "$HL_DEDUP_PCT" == "0" || -z "$HL_DEDUP_PCT" ]] && HL_DEDUP_PCT="-"

# Format cells with suffixes, falling back to "-" for unavailable values
HL_FMT="  %-18s| %14s | %25s | %8s | %11s | %s\n"
HL_SEP="  %-18s|%s|%s|%s|%s|%s\n"

HL_SF_THRU="${HIGHLIGHT_RPS} rows/s"; [[ "$HIGHLIGHT_RPS" == "-" ]] && HL_SF_THRU="-"
HL_SF_VOL="${HL_SF_ROWS} rows";      [[ "$HL_SF_ROWS" == "-" ]]    && HL_SF_VOL="-"
HL_SF_DUR="${HL_SF_MS}ms";           [[ "$HL_SF_MS" == "-" ]]      && HL_SF_DUR="-"
HL_SF_BATCH_STR="avg~${HL_SF_BATCH}"; [[ "$HL_SF_BATCH" == "-" ]] && HL_SF_BATCH_STR="-"
HL_SF_CONC="${CW_PEAK_CONC} (peak)"; [[ "$CW_PEAK_CONC" == "-" ]] && HL_SF_CONC="-"

echo ""
log "── Highlights ──────────────────────────────────────────────────────────────────────────"
printf "$HL_FMT" "PIPELINE" "THROUGHPUT" "VOLUME" "DURATION" "BATCH" "CONCURRENCY"
printf "$HL_SEP" "------------------" "----------------" "---------------------------" "----------" "-------------" "------------"
printf "$HL_FMT" "Snowflake -> l" "$HL_SF_THRU" "$HL_SF_VOL" "$HL_SF_DUR" "$HL_SF_BATCH_STR" "$HL_SF_CONC"
if $SKYFLOW_MODE; then
  HL_SK_THRU="${HL_SF_TPS} tok/s";  [[ "$HL_SF_TPS" == "-" ]]        && HL_SK_THRU="-"
  HL_SK_VOL="${HL_TOTAL_TOKENS} tok, ${HL_DEDUP_PCT}%dd"
  [[ "$HL_TOTAL_TOKENS" == "-" ]] && HL_SK_VOL="-"
  printf "$HL_FMT" "Lambda -> Skyflow" "$HL_SK_THRU" "$HL_SK_VOL" "-" "${SKYFLOW_BATCH_SIZE:-"-"}" "${SKYFLOW_CONCURRENCY:-"-"}"
fi

echo ""
log "──────────────────────────────────────────────────"
log "Benchmark complete!"
log "Results: ${SF_DB}.${SF_SCHEMA}.benchmark_results"
log "Logs:    /aws/lambda/${LAMBDA_NAME}"
echo ""
log "Next run:"
log "  Change scale:  $0 --skip-deploy --rows N --unique-tokens N ..."
log "  Same config:   $0 --skip-deploy --skip-setup"
log "  Cleanup:       $0 --cleanup"
