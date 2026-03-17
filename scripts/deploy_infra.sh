#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT_DIR}/producer/.env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing ${ENV_FILE}. Create it from producer/.env.example first."
  exit 1
fi

set -a
source "$ENV_FILE"
set +a

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "Required command not found: $1"; exit 1; }
}

require_cmd confluent
require_cmd python3

CC_ENV_NAME="${CC_ENV_NAME:-confluent-ai-day-2026}"
KAFKA_CLUSTER_NAME="${KAFKA_CLUSTER_NAME:-heart-rate-hackathon}"
KAFKA_CLOUD="${KAFKA_CLOUD:-aws}"
KAFKA_REGION="${KAFKA_REGION:-us-west-2}"
KAFKA_CLUSTER_TYPE="${KAFKA_CLUSTER_TYPE:-basic}"
FLINK_POOL_NAME="${FLINK_POOL_NAME:-heart-rate-flink-pool}"
FLINK_MAX_CFU="${FLINK_MAX_CFU:-5}"
TOPICS_CSV="${TOPICS:-heart-rate-raw,heart-rate-cleaned,heart-rate-features,heart-rate-risk-events,patient.heart_state.snapshot}"
TOPIC_PARTITIONS="${TOPIC_PARTITIONS:-6}"

json_get() {
  local py_expr="$1"
  python3 -c "import json,sys; data=json.load(sys.stdin); print(${py_expr})"
}

echo "[1/7] Ensure environment: ${CC_ENV_NAME}"
ENV_JSON="$(confluent environment list -o json)"
ENV_ID="$(printf '%s' "$ENV_JSON" | python3 -c 'import json,sys; name=sys.argv[1]; arr=json.load(sys.stdin); print(next((x.get("id") for x in arr if x.get("name")==name), ""))' "$CC_ENV_NAME")"

if [[ -z "$ENV_ID" ]]; then
  ENV_ID="$(confluent environment create "$CC_ENV_NAME" -o json | json_get "data['id']")"
  echo "  created env: ${ENV_ID}"
else
  echo "  found env: ${ENV_ID}"
fi

confluent environment use "$ENV_ID" >/dev/null

echo "[2/7] Ensure Kafka cluster: ${KAFKA_CLUSTER_NAME}"
KAFKA_LIST_JSON="$(confluent kafka cluster list --environment "$ENV_ID" -o json)"
KAFKA_CLUSTER_ID="$(printf '%s' "$KAFKA_LIST_JSON" | python3 -c 'import json,sys; name=sys.argv[1]; arr=json.load(sys.stdin); print(next((x.get("id") for x in arr if x.get("name")==name), ""))' "$KAFKA_CLUSTER_NAME")"

if [[ -z "$KAFKA_CLUSTER_ID" ]]; then
  KAFKA_CLUSTER_ID="$(confluent kafka cluster create "$KAFKA_CLUSTER_NAME" --cloud "$KAFKA_CLOUD" --region "$KAFKA_REGION" --type "$KAFKA_CLUSTER_TYPE" --environment "$ENV_ID" -o json | json_get "data['id']")"
  echo "  created cluster: ${KAFKA_CLUSTER_ID}"
else
  echo "  found cluster: ${KAFKA_CLUSTER_ID}"
fi

confluent kafka cluster use "$KAFKA_CLUSTER_ID" >/dev/null

echo "[3/7] Ensure topics"
IFS=',' read -r -a TOPICS <<< "$TOPICS_CSV"
for topic in "${TOPICS[@]}"; do
  t="$(echo "$topic" | xargs)"
  [[ -z "$t" ]] && continue
  if confluent kafka topic describe "$t" >/dev/null 2>&1; then
    echo "  exists: $t"
  else
    confluent kafka topic create "$t" --partitions "$TOPIC_PARTITIONS" >/dev/null
    echo "  created: $t"
  fi
done

echo "[4/7] Ensure Flink compute pool: ${FLINK_POOL_NAME}"
POOL_LIST_JSON="$(confluent flink compute-pool list --environment "$ENV_ID" -o json)"
FLINK_POOL_ID="$(printf '%s' "$POOL_LIST_JSON" | python3 -c 'import json,sys; name=sys.argv[1]; arr=json.load(sys.stdin); print(next((x.get("id") for x in arr if x.get("name")==name), ""))' "$FLINK_POOL_NAME")"

if [[ -z "$FLINK_POOL_ID" ]]; then
  FLINK_POOL_ID="$(confluent flink compute-pool create "$FLINK_POOL_NAME" --cloud "$KAFKA_CLOUD" --region "$KAFKA_REGION" --max-cfu "$FLINK_MAX_CFU" --environment "$ENV_ID" -o json | json_get "data['id']")"
  echo "  created pool: ${FLINK_POOL_ID}"
else
  echo "  found pool: ${FLINK_POOL_ID}"
fi

confluent flink compute-pool use "$FLINK_POOL_ID" >/dev/null

recreate_statement() {
  local statement_name="$1"
  local sql_text="$2"
  confluent flink statement delete "$statement_name" --environment "$ENV_ID" >/dev/null 2>&1 || true
  confluent flink statement create "$statement_name" --sql "$sql_text" --environment "$ENV_ID" --database "$KAFKA_CLUSTER_ID" --wait -o json >/dev/null
}

echo "[5/7] Reset managed tables"
for table in "heart-rate-raw" "heart-rate-cleaned" "heart-rate-features" "heart-rate-risk-events"; do
  recreate_statement "drop-${table}-auto" "DROP TABLE IF EXISTS \`${table}\`;"
  echo "  dropped (if existed): ${table}"
done

echo "[6/7] Create tables"
create_table() {
  local table_name="$1"
  local sql
  sql="$(awk -v t="$table_name" 'BEGIN{f=0} $0 ~ "^CREATE TABLE `"t"` "{f=1} f{print} f && $0 ~ /^\);[[:space:]]*$/ {exit}' "$ROOT_DIR/flink/sql/00_tables.sql")"
  recreate_statement "ddl-${table_name}-auto" "$sql"
  echo "  created: ${table_name}"
}

create_table "heart-rate-raw"
create_table "heart-rate-cleaned"
create_table "heart-rate-features"
create_table "heart-rate-risk-events"

echo "[7/7] Start jobs"
recreate_statement job-heart-cleaning "$(cat "$ROOT_DIR/flink/sql/10_cleaning.sql")"
recreate_statement job-heart-window "$(cat "$ROOT_DIR/flink/sql/20_window_features.sql")"
recreate_statement job-heart-anomaly "$(cat "$ROOT_DIR/flink/sql/30_anomaly_detection.sql")"

echo "Deployment complete."
echo "Environment: ${ENV_ID}"
echo "Kafka cluster: ${KAFKA_CLUSTER_ID}"
echo "Flink pool: ${FLINK_POOL_ID}"
