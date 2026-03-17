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
require_cmd gradle

TOTAL_EVENTS_TEST="${TOTAL_EVENTS_TEST:-30}"
EVENTS_PER_SECOND_TEST="${EVENTS_PER_SECOND_TEST:-1}"
HEART_RATE_MODE_TEST="${HEART_RATE_MODE_TEST:-spiky}"

CC_ENV_ID="${CC_ENV_ID:-env-vkmvdj}"
KAFKA_CLUSTER_ID="${KAFKA_CLUSTER_ID:-lkc-wnnk6m}"

echo "[1/5] Verify Flink jobs are running"
for job in job-heart-cleaning job-heart-window job-heart-anomaly; do
  confluent flink statement describe "$job" --environment "$CC_ENV_ID" -o json | python3 -c 'import json,sys; s=json.load(sys.stdin); name=s.get("name"); status=s.get("status"); print(f"  {name}: {status}"); raise SystemExit(0 if status=="RUNNING" else 1)'
done

echo "[2/5] Produce finite test batch"
(
  cd "$ROOT_DIR/producer"
  TOTAL_EVENTS="$TOTAL_EVENTS_TEST" EVENTS_PER_SECOND="$EVENTS_PER_SECOND_TEST" HEART_RATE_MODE="$HEART_RATE_MODE_TEST" ./run-producer.sh
)

echo "[3/5] Sample cleaned output"
confluent kafka topic consume heart-rate-cleaned \
  --bootstrap "SASL_SSL://$BOOTSTRAP_SERVERS" \
  --api-key "$KAFKA_API_KEY" \
  --api-secret "$KAFKA_API_SECRET" \
  --schema-registry-endpoint "$SCHEMA_REGISTRY_URL" \
  --schema-registry-api-key "$SR_API_KEY" \
  --schema-registry-api-secret "$SR_API_SECRET" \
  --value-format jsonschema \
  --from-beginning --print-offset | head -n 6 || true

echo "[4/5] Sample risk output"
confluent kafka topic consume heart-rate-risk-events \
  --bootstrap "SASL_SSL://$BOOTSTRAP_SERVERS" \
  --api-key "$KAFKA_API_KEY" \
  --api-secret "$KAFKA_API_SECRET" \
  --schema-registry-endpoint "$SCHEMA_REGISTRY_URL" \
  --schema-registry-api-key "$SR_API_KEY" \
  --schema-registry-api-secret "$SR_API_SECRET" \
  --value-format jsonschema \
  --from-beginning --print-offset | head -n 6 || true

echo "[5/5] Sample feature output (may require full 1-minute window close)"
confluent kafka topic consume heart-rate-features \
  --bootstrap "SASL_SSL://$BOOTSTRAP_SERVERS" \
  --api-key "$KAFKA_API_KEY" \
  --api-secret "$KAFKA_API_SECRET" \
  --schema-registry-endpoint "$SCHEMA_REGISTRY_URL" \
  --schema-registry-api-key "$SR_API_KEY" \
  --schema-registry-api-secret "$SR_API_SECRET" \
  --value-format jsonschema \
  --from-beginning --print-offset | head -n 6 || true

echo "Pipeline test complete."
