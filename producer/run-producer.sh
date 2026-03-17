#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [[ ! -f .env ]]; then
  echo "Missing .env file. Create one from .env.example:"
  echo "  cp .env.example .env"
  exit 1
fi

while IFS= read -r line || [[ -n "$line" ]]; do
  [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
  key="${line%%=*}"
  value="${line#*=}"
  [[ -z "$key" ]] && continue
  if [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    if [[ -z "${!key+x}" ]]; then
      export "$key=$value"
    fi
  fi
done < .env

if [[ -z "${KAFKA_API_KEY:-}" || -z "${KAFKA_API_SECRET:-}" || -z "${SR_API_KEY:-}" || -z "${SR_API_SECRET:-}" ]]; then
  echo "Missing one or more required creds in .env: KAFKA_API_KEY, KAFKA_API_SECRET, SR_API_KEY, SR_API_SECRET"
  exit 1
fi

export SASL_JAAS_CONFIG="org.apache.kafka.common.security.plain.PlainLoginModule required username='${KAFKA_API_KEY}' password='${KAFKA_API_SECRET}';"
export SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO="${SR_API_KEY}:${SR_API_SECRET}"

echo "Running producer against: ${BOOTSTRAP_SERVERS}"
echo "Effective settings: topic=${RAW_TOPIC:-heart-rate-raw} events_per_second=${EVENTS_PER_SECOND:-1} total_events=${TOTAL_EVENTS:-0} mode=${HEART_RATE_MODE:-normal} patient=${PATIENT_ID:-user-00987}"
gradle run
