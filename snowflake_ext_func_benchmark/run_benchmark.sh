#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# Snowflake External Function Throughput Benchmark
#
# Measures how Snowflake batches and parallelizes external function calls.
# Single script — zero manual steps.
#
# Usage: ./run_benchmark.sh [options]
###############################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ─── Defaults ────────────────────────────────────────────────────────────────
REGION="us-east-2"
AWS_PROFILE=""
SF_CONNECTION="default"
SKIP_DEPLOY=false
SKIP_SETUP=false
CLEANUP=false
DELAY_MS=0
QUICK=false
INSTALL_PREREQS=false

# Resource naming
# AWS resources use hyphens, Snowflake uses underscores (SF rejects hyphens in identifiers)
AWS_PREFIX="ext-func-bench"
SF_PREFIX="ext_func_bench"
LAMBDA_NAME="${AWS_PREFIX}-detokenize"
DYNAMO_TABLE="ext_func_benchmark_metrics"
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
  esac
}

ALL_WAREHOUSES=(BENCH_XS BENCH_M BENCH_XL BENCH_2XL)
ALL_TABLES=(test_tokens_10m test_tokens_100m test_tokens_500m)
ALL_BATCH_CONFIGS=(default 10000)

TABLE_ROWS_test_tokens_10m=10000000
TABLE_ROWS_test_tokens_100m=100000000
TABLE_ROWS_test_tokens_500m=500000000

# ─── Parse arguments ─────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case $1 in
    --region)      REGION="$2"; shift 2 ;;
    --profile)     AWS_PROFILE="$2"; shift 2 ;;
    --connection)  SF_CONNECTION="$2"; shift 2 ;;
    --skip-deploy) SKIP_DEPLOY=true; shift ;;
    --skip-setup)  SKIP_SETUP=true; shift ;;
    --cleanup)     CLEANUP=true; shift ;;
    --delay-ms)    DELAY_MS="$2"; shift 2 ;;
    --quick)           QUICK=true; shift ;;
    --install-prereqs) INSTALL_PREREQS=true; shift ;;
    -h|--help)
      echo "Usage: $0 [options]"
      echo "  --region REGION        AWS region (default: us-east-2)"
      echo "  --profile PROFILE      AWS CLI profile name (e.g., AdministratorAccess-571930033416)"
      echo "  --connection NAME      Snowflake CLI connection name (default: default)"
      echo "  --skip-deploy          Reuse existing AWS infrastructure"
      echo "  --skip-setup           Reuse existing Snowflake objects"
      echo "  --cleanup              Tear down everything and exit"
      echo "  --delay-ms MS          Simulated API latency in Lambda (default: 0)"
      echo "  --quick                Reduced test matrix (XS/XL, 10m/100m, default/10000)"
      echo "  --install-prereqs      Install missing prerequisites via Homebrew/pip"
      exit 0
      ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# Quick mode reduces the matrix
if $QUICK; then
  ALL_WAREHOUSES=(BENCH_XS BENCH_XL)
  ALL_TABLES=(test_tokens_10m test_tokens_100m)
  ALL_BATCH_CONFIGS=(default 10000)
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
  aws_ lambda delete-function --function-name "$LAMBDA_NAME" > /dev/null 2>&1 || true
  ok "Lambda deleted"

  log "Deleting DynamoDB table..."
  aws_ dynamodb delete-table --table-name "$DYNAMO_TABLE" > /dev/null 2>&1 || true
  ok "DynamoDB table deleted"

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
    GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o bootstrap main.go
  )
  (cd "${SCRIPT_DIR}/lambda" && zip -j "${SCRIPT_DIR}/lambda.zip" bootstrap) > /dev/null
  rm -f "${SCRIPT_DIR}/lambda/bootstrap"
  ok "Lambda binary built"

  # ── DynamoDB ──
  log "Creating DynamoDB table..."
  aws_ dynamodb create-table \
    --table-name "$DYNAMO_TABLE" \
    --attribute-definitions \
      AttributeName=query_id,AttributeType=S \
      AttributeName=sk,AttributeType=S \
    --key-schema \
      AttributeName=query_id,KeyType=HASH \
      AttributeName=sk,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    > /dev/null 2>&1 || warn "DynamoDB table may already exist"
  ok "DynamoDB table: ${DYNAMO_TABLE}"

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
    },
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:PutItem"
      ],
      "Resource": "arn:aws:dynamodb:${REGION}:${AWS_ACCOUNT_ID}:table/${DYNAMO_TABLE}"
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
  # Try to create; if it exists, update
  if aws_ lambda create-function \
    --function-name "$LAMBDA_NAME" \
    --runtime provided.al2023 \
    --handler bootstrap \
    --role "$LAMBDA_ROLE_ARN" \
    --zip-file "fileb://${SCRIPT_DIR}/lambda.zip" \
    --memory-size 256 \
    --timeout 30 \
    --environment "Variables={DYNAMODB_TABLE=${DYNAMO_TABLE},SIMULATED_DELAY_MS=${DELAY_MS}}" \
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
      --environment "Variables={DYNAMODB_TABLE=${DYNAMO_TABLE},SIMULATED_DELAY_MS=${DELAY_MS}}" \
      > /dev/null 2>&1
    ok "Lambda updated: ${LAMBDA_NAME}"
  fi

  # Set reserved concurrency (account limit=1000, must leave 100 unreserved)
  aws_ lambda put-function-concurrency \
    --function-name "$LAMBDA_NAME" \
    --reserved-concurrent-executions 900 \
    > /dev/null
  ok "Lambda reserved concurrency: 900"

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

  # Create /detokenize resource (idempotent-ish)
  RESOURCE_ID=$(aws_ apigateway get-resources --rest-api-id "$API_ID" --query "items[?pathPart=='detokenize'].id | [0]" --output text 2>/dev/null || true)
  if [[ ! "$RESOURCE_ID" =~ ^[a-z0-9]+$ ]]; then
    RESOURCE_ID=$(aws_ apigateway create-resource \
      --rest-api-id "$API_ID" \
      --parent-id "$ROOT_ID" \
      --path-part detokenize \
      --query 'id' --output text)
  fi
  ok "Resource /detokenize: ${RESOURCE_ID}"

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
# Phase 3: Configure Snowflake
###############################################################################
if $SKIP_SETUP; then
  log "PHASE 3: Skipping Snowflake setup (--skip-setup)"
else
  log "PHASE 3: Configuring Snowflake"

  # ── Database and schema ──
  snow_sql_silent "CREATE DATABASE IF NOT EXISTS ${SF_DB}"
  snow_sql_silent "CREATE SCHEMA IF NOT EXISTS ${SF_DB}.${SF_SCHEMA}"
  ok "Database: ${SF_DB}.${SF_SCHEMA}"

  # ── API Integration ──
  log "Creating API integration..."
  INTEGRATION_NAME="${SF_PREFIX}_api_integration"

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

  FUNC_PREFIX="${SF_DB}.${SF_SCHEMA}"

  # Default (no MAX_BATCH_ROWS)
  snow_sql_silent "CREATE OR REPLACE EXTERNAL FUNCTION ${FUNC_PREFIX}.benchmark_detokenize_default(token_value VARCHAR)
    RETURNS VARIANT
    API_INTEGRATION = ${INTEGRATION_NAME}
    HEADERS = ('sf-benchmark-config' = 'default')
    CONTEXT_HEADERS = (CURRENT_TIMESTAMP)
    AS '${API_URL}/detokenize'"
  ok "Function: benchmark_detokenize_default"

  for BATCH in 100 1000 2000 10000; do
    snow_sql_silent "CREATE OR REPLACE EXTERNAL FUNCTION ${FUNC_PREFIX}.benchmark_detokenize_${BATCH}(token_value VARCHAR)
      RETURNS VARIANT
      API_INTEGRATION = ${INTEGRATION_NAME}
      MAX_BATCH_ROWS = ${BATCH}
      HEADERS = ('sf-benchmark-config' = 'batch_${BATCH}')
      CONTEXT_HEADERS = (CURRENT_TIMESTAMP)
      AS '${API_URL}/detokenize'"
    ok "Function: benchmark_detokenize_${BATCH} (MAX_BATCH_ROWS=${BATCH})"
  done

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

  # ── Test data (use 2XL if available, else XL, else largest WH for speed) ──
  DATA_GEN_WH=""
  for candidate in BENCH_2XL BENCH_XL BENCH_L BENCH_M BENCH_S BENCH_XS; do
    for wh in "${ALL_WAREHOUSES[@]}"; do
      if [[ "$wh" == "$candidate" ]]; then DATA_GEN_WH="$candidate"; break 2; fi
    done
  done
  log "Generating test data tables (using warehouse ${DATA_GEN_WH})..."

  for tbl in "${ALL_TABLES[@]}"; do
    row_var="TABLE_ROWS_${tbl}"
    rows=${!row_var}
    log "  Generating ${tbl} (${rows} rows)..."
    snow_sql_silent "USE WAREHOUSE ${DATA_GEN_WH};
      CREATE OR REPLACE TABLE ${FUNC_PREFIX}.${tbl} AS
      SELECT
        SEQ4() AS id,
        UUID_STRING() AS token_value,
        'extra_data_' || SEQ4()::VARCHAR AS extra_col
      FROM TABLE(GENERATOR(ROWCOUNT => ${rows}))"
    ok "Table: ${tbl} (${rows} rows)"
  done

  # ── Results table ──
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
    lambda_invocations INTEGER,
    lambda_total_rows INTEGER,
    lambda_unique_instances INTEGER,
    lambda_avg_batch INTEGER,
    lambda_max_batch INTEGER,
    lambda_min_batch INTEGER,
    lambda_time_spread_ms INTEGER,
    lambda_avg_duration_ms FLOAT,
    sf_rows_per_sec FLOAT,
    simulated_delay_ms INTEGER
  )"
  ok "Results table created"

  # ── Smoke test ──
  log "Running smoke test..."
  SMOKE_RESULT=$(snow_sql "SELECT ${FUNC_PREFIX}.benchmark_detokenize_default('test-token-123') AS result" 2>&1 || true)
  if echo "$SMOKE_RESULT" | grep -qi "DETOK_"; then
    ok "Smoke test passed — external function is working"
  else
    err "Smoke test did not return expected DETOK_ prefix"
    echo "Smoke test output:"
    echo "$SMOKE_RESULT"
    echo ""
    warn "Debug tips:"
    echo "  1. Check API Gateway is deployed: curl -X POST ${API_URL}/detokenize"
    echo "  2. Check Lambda logs: aws logs tail /aws/lambda/${LAMBDA_NAME} --region ${REGION}"
    echo "  3. Check integration: snowsql -c ${SF_CONNECTION} -q \"DESCRIBE INTEGRATION ${INTEGRATION_NAME}\""
    echo "  4. Verify IAM trust: aws iam get-role --role-name ${SF_ROLE_NAME} --region ${REGION}"
    die "Smoke test failed. Fix the issue and re-run with --skip-deploy."
  fi
fi

echo ""

###############################################################################
# Phase 4: Run Benchmarks
###############################################################################
log "PHASE 4: Running benchmarks (delay=${DELAY_MS}ms)"

FUNC_PREFIX="${SF_DB}.${SF_SCHEMA}"

# Build test matrix
declare -a MATRIX=()
for wh in "${ALL_WAREHOUSES[@]}"; do
  for tbl in "${ALL_TABLES[@]}"; do
    for batch in "${ALL_BATCH_CONFIGS[@]}"; do
      MATRIX+=("${wh}|${tbl}|${batch}")
    done
  done
done

TOTAL=${#MATRIX[@]}
CURRENT=0

log "Test matrix: ${TOTAL} combinations"
echo ""

printf "%-8s | %-18s | %-7s | %8s | %8s | %6s | %8s | %5s | %12s\n" \
  "WH" "TABLE" "BATCH" "WALL_MS" "SF_MS" "CALLS" "AVG_BAT" "CONC" "SF_ROWS/SEC"
printf "%s\n" \
  "---------|--------------------|---------|---------|---------+--------+---------|-------|-----------"

for entry in "${MATRIX[@]}"; do
  IFS='|' read -r wh tbl batch <<< "$entry"
  CURRENT=$((CURRENT + 1))

  size="$(wh_size "$wh")"
  row_var="TABLE_ROWS_${tbl}"
  rows=${!row_var}

  # Determine function name
  if [[ "$batch" == "default" ]]; then
    func="benchmark_detokenize_default"
  else
    func="benchmark_detokenize_${batch}"
  fi

  # ── Single snowsql session: warmup + benchmark + metrics ──
  # All statements run in ONE session so LAST_QUERY_ID() correctly
  # references the benchmark query. snowsql -o output_format=json
  # emits one JSON array per statement; we grab the LAST one (metrics).
  BENCH_SQL="
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

  START_NS=$(nanos)
  BENCH_OUTPUT=$(snowsql "${SNOWSQL_OPTS[@]}" -q "$BENCH_SQL" -o output_format=json 2>/dev/null || true)
  WALLCLOCK_MS=$(millis_since "$START_NS")

  # Last JSON array = metrics query result
  METRICS_JSON=$(echo "$BENCH_OUTPUT" | grep '^\[' | tail -1)

  # Parse Snowflake-side metrics (QUERY_ID + SF_ELAPSED_MS)
  # Note: INFORMATION_SCHEMA.QUERY_HISTORY doesn't populate external function
  # columns — we get parallelism data from DynamoDB instead (see below).
  set +e
  QUERY_ID=$(echo "$METRICS_JSON" | jq -r '.[0].QUERY_ID // "unknown"' 2>/dev/null)
  SF_ELAPSED=$(echo "$METRICS_JSON" | jq -r '.[0].SF_ELAPSED_MS // 0' 2>/dev/null)
  set -e
  QUERY_ID=${QUERY_ID:-unknown}; QUERY_ID=${QUERY_ID//null/unknown}
  SF_ELAPSED=${SF_ELAPSED:-0}; SF_ELAPSED=${SF_ELAPSED//null/0}

  # ── Query DynamoDB for Lambda-side metrics ──
  # Lambda writes one record per invocation with batch_size, timestamps,
  # and instance ID. This gives us real-time parallelism data.
  # Use --consistent-read since Lambda writes are synchronous.
  LAMBDA_INVOCATIONS=0; LAMBDA_TOTAL_ROWS=0; LAMBDA_UNIQUE_INSTANCES=0
  LAMBDA_AVG_BATCH=0; LAMBDA_MAX_BATCH=0; LAMBDA_MIN_BATCH=0
  LAMBDA_TIME_SPREAD_MS=0; LAMBDA_AVG_DURATION_MS=0

  if [[ "$QUERY_ID" != "unknown" ]]; then
    sleep 1  # brief buffer for DynamoDB consistency
    set +e
    DDB_RESULT=$(aws_ dynamodb query \
      --table-name "$DYNAMO_TABLE" \
      --key-condition-expression "query_id = :qid" \
      --expression-attribute-values "{\":qid\":{\"S\":\"${QUERY_ID}\"}}" \
      --consistent-read \
      --output json 2>/tmp/ddb_err.log)
    DDB_RC=$?
    set -e
    if [[ $DDB_RC -ne 0 ]]; then
      DDB_RESULT='{"Count":0,"Items":[]}'
      if grep -qi "expired\|token\|credential" /tmp/ddb_err.log 2>/dev/null; then
        warn "AWS credentials expired — DynamoDB metrics unavailable. Refresh credentials and re-run."
      fi
    fi

    set +e
    LAMBDA_INVOCATIONS=$(echo "$DDB_RESULT" | jq '.Count // 0')
    LAMBDA_TOTAL_ROWS=$(echo "$DDB_RESULT" | jq '[.Items[]?.batch_size.N // "0" | tonumber] | add // 0')
    LAMBDA_UNIQUE_INSTANCES=$(echo "$DDB_RESULT" | jq '[.Items[]?.lambda_instance_id.S // empty] | unique | length')
    LAMBDA_AVG_BATCH=$(echo "$DDB_RESULT" | jq 'if .Count > 0 then ([.Items[].batch_size.N // "0" | tonumber] | add) / .Count | floor else 0 end')
    LAMBDA_MAX_BATCH=$(echo "$DDB_RESULT" | jq '[.Items[].batch_size.N // "0" | tonumber] | max // 0')
    LAMBDA_MIN_BATCH=$(echo "$DDB_RESULT" | jq '[.Items[].batch_size.N // "0" | tonumber] | min // 0')
    LAMBDA_TIME_SPREAD_MS=$(echo "$DDB_RESULT" | jq 'if .Count > 1 then (([.Items[].receive_timestamp_ns.N // "0" | tonumber] | max) - ([.Items[].receive_timestamp_ns.N // "0" | tonumber] | min)) / 1000000 | floor else 0 end')
    LAMBDA_AVG_DURATION_MS=$(echo "$DDB_RESULT" | jq 'if .Count > 0 then ([.Items[].processing_duration_ns.N // "0" | tonumber] | add) / .Count / 1000000 else 0 end | . * 10 | floor / 10')
    set -e
  fi

  # Derived metrics
  if [[ "$SF_ELAPSED" -gt 0 ]]; then
    SF_ROWS_PER_SEC=$(( rows * 1000 / SF_ELAPSED ))
  else
    SF_ROWS_PER_SEC=0
  fi

  # Store results
  snow_sql_silent "INSERT INTO ${FUNC_PREFIX}.benchmark_results
    (warehouse_name, warehouse_size, row_count, batch_config, query_id,
     wallclock_ms, sf_elapsed_ms,
     lambda_invocations, lambda_total_rows, lambda_unique_instances,
     lambda_avg_batch, lambda_max_batch, lambda_min_batch,
     lambda_time_spread_ms, lambda_avg_duration_ms,
     sf_rows_per_sec, simulated_delay_ms)
    VALUES ('${wh}', '${size}', ${rows}, '${batch}', '${QUERY_ID}',
     ${WALLCLOCK_MS}, ${SF_ELAPSED},
     ${LAMBDA_INVOCATIONS}, ${LAMBDA_TOTAL_ROWS}, ${LAMBDA_UNIQUE_INSTANCES},
     ${LAMBDA_AVG_BATCH}, ${LAMBDA_MAX_BATCH}, ${LAMBDA_MIN_BATCH},
     ${LAMBDA_TIME_SPREAD_MS}, ${LAMBDA_AVG_DURATION_MS},
     ${SF_ROWS_PER_SEC}, ${DELAY_MS})"

  # Print progress
  printf "%-8s | %-18s | %-7s | %6dms | %6dms | %6d | %8d | %5d | %10d\n" \
    "$wh" "$tbl" "$batch" "$WALLCLOCK_MS" "$SF_ELAPSED" "$LAMBDA_INVOCATIONS" "$LAMBDA_AVG_BATCH" "$LAMBDA_UNIQUE_INSTANCES" "$SF_ROWS_PER_SEC"
done

echo ""

###############################################################################
# Phase 5: Display Results
###############################################################################
log "PHASE 5: Results Summary"
echo ""

# ── Full results table ──
log "Benchmark Results:"
snow_sql "SELECT
    warehouse_name AS wh,
    row_count AS rows,
    batch_config AS batch,
    sf_elapsed_ms AS sf_ms,
    lambda_invocations AS calls,
    lambda_unique_instances AS parallel,
    lambda_avg_batch AS avg_batch,
    ROUND(sf_rows_per_sec, 0) AS sf_rps,
    query_id
  FROM ${FUNC_PREFIX}.benchmark_results
  WHERE simulated_delay_ms = ${DELAY_MS}
  ORDER BY warehouse_size, row_count, batch_config"
echo ""

# ── Pivot: SF rows/sec by warehouse × batch config (for largest table) ──
log "Pivot: SF Rows/sec by Warehouse x Batch Config (100m rows, delay=${DELAY_MS}ms):"
snow_sql "SELECT
    batch_config AS batch,
    MAX(CASE WHEN warehouse_name = 'BENCH_XS' THEN ROUND(sf_rows_per_sec, 0) END) AS XS,
    MAX(CASE WHEN warehouse_name = 'BENCH_M' THEN ROUND(sf_rows_per_sec, 0) END) AS M,
    MAX(CASE WHEN warehouse_name = 'BENCH_XL' THEN ROUND(sf_rows_per_sec, 0) END) AS XL,
    MAX(CASE WHEN warehouse_name = 'BENCH_2XL' THEN ROUND(sf_rows_per_sec, 0) END) AS XXL
  FROM ${FUNC_PREFIX}.benchmark_results
  WHERE row_count = 100000000 AND simulated_delay_ms = ${DELAY_MS}
  GROUP BY batch_config
  ORDER BY batch_config"
echo ""

# ── Parallelism analysis: invocations and batch sizes ──
log "Parallelism Analysis (from Lambda-side DynamoDB metrics):"
snow_sql "SELECT
    warehouse_name AS wh,
    row_count AS rows,
    batch_config AS batch,
    sf_elapsed_ms AS sf_ms,
    lambda_invocations AS calls,
    lambda_unique_instances AS concurrency,
    lambda_avg_batch AS avg_batch,
    lambda_min_batch AS min_batch,
    lambda_max_batch AS max_batch,
    lambda_time_spread_ms AS spread_ms,
    ROUND(lambda_avg_duration_ms, 1) AS avg_lambda_ms,
    ROUND(sf_rows_per_sec, 0) AS sf_rps
  FROM ${FUNC_PREFIX}.benchmark_results
  WHERE simulated_delay_ms = ${DELAY_MS}
  ORDER BY warehouse_size, row_count, batch_config"
echo ""

# ── Cost analysis using Snowflake-side timing ──
log "Cost Analysis (estimated credits per million rows, based on SF elapsed time):"
snow_sql "SELECT
    warehouse_name AS wh,
    warehouse_size AS size,
    row_count AS rows,
    batch_config AS batch,
    sf_elapsed_ms AS sf_ms,
    lambda_unique_instances AS concurrency,
    ROUND(sf_rows_per_sec, 0) AS sf_rps,
    CASE warehouse_size
      WHEN 'XSMALL' THEN 1 WHEN 'SMALL' THEN 2 WHEN 'MEDIUM' THEN 4
      WHEN 'LARGE' THEN 8 WHEN 'XLARGE' THEN 16 WHEN 'XXLARGE' THEN 32
    END AS cr_per_hr,
    ROUND(
      CASE warehouse_size
        WHEN 'XSMALL' THEN 1 WHEN 'SMALL' THEN 2 WHEN 'MEDIUM' THEN 4
        WHEN 'LARGE' THEN 8 WHEN 'XLARGE' THEN 16 WHEN 'XXLARGE' THEN 32
      END * (1000000.0 / NULLIF(sf_rows_per_sec, 0)) / 3600.0,
      4
    ) AS cr_per_1m_rows
  FROM ${FUNC_PREFIX}.benchmark_results
  WHERE simulated_delay_ms = ${DELAY_MS}
  ORDER BY cr_per_1m_rows NULLS LAST"
echo ""

# ── CloudWatch metrics (best-effort) ──
log "CloudWatch Lambda Metrics (last 30 minutes):"
END_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || gdate -u +%Y-%m-%dT%H:%M:%SZ)
START_TIME=$(date -u -v-30M +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || gdate -u -d '30 minutes ago' +%Y-%m-%dT%H:%M:%SZ)

echo "  Peak concurrent executions:"
aws_ cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name ConcurrentExecutions \
  --dimensions Name=FunctionName,Value="${LAMBDA_NAME}" \
  --start-time "$START_TIME" \
  --end-time "$END_TIME" \
  --period 60 \
  --statistics Maximum \
  --query 'sort_by(Datapoints, &Timestamp)[-5:].{Time:Timestamp,Max:Maximum}' \
  --output table 2>/dev/null || warn "Could not fetch ConcurrentExecutions metric"

echo ""
echo "  Invocations per minute:"
aws_ cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Invocations \
  --dimensions Name=FunctionName,Value="${LAMBDA_NAME}" \
  --start-time "$START_TIME" \
  --end-time "$END_TIME" \
  --period 60 \
  --statistics Sum \
  --query 'sort_by(Datapoints, &Timestamp)[-5:].{Time:Timestamp,Count:Sum}' \
  --output table 2>/dev/null || warn "Could not fetch Invocations metric"

echo ""
echo "  Throttles:"
aws_ cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Throttles \
  --dimensions Name=FunctionName,Value="${LAMBDA_NAME}" \
  --start-time "$START_TIME" \
  --end-time "$END_TIME" \
  --period 60 \
  --statistics Sum \
  --query 'sort_by(Datapoints, &Timestamp)[-5:].{Time:Timestamp,Throttles:Sum}' \
  --output table 2>/dev/null || warn "Could not fetch Throttles metric"

echo ""
log "Benchmark complete!"
log "Results stored in: ${SF_DB}.${SF_SCHEMA}.benchmark_results"
log "Lambda metrics in DynamoDB: ${DYNAMO_TABLE}"
log "To re-run with latency: $0 --skip-deploy --skip-setup --delay-ms 100"
log "To clean up: $0 --cleanup"
