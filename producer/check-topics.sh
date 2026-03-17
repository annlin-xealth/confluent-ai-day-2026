#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [[ ! -f .env ]]; then
  echo "Missing .env file. Create one from .env.example:"
  echo "  cp .env.example .env"
  exit 1
fi

set -a
source .env
set +a

export SASL_JAAS_CONFIG="org.apache.kafka.common.security.plain.PlainLoginModule required username='${KAFKA_API_KEY}' password='${KAFKA_API_SECRET}';"
export SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO="${SR_API_KEY}:${SR_API_SECRET}"

if [[ -z "${BOOTSTRAP_SERVERS:-}" || -z "${SCHEMA_REGISTRY_URL:-}" || -z "${KAFKA_API_KEY:-}" || -z "${KAFKA_API_SECRET:-}" || -z "${SR_API_KEY:-}" || -z "${SR_API_SECRET:-}" ]]; then
  echo "Missing required values in .env. Required: BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, KAFKA_API_KEY, KAFKA_API_SECRET, SR_API_KEY, SR_API_SECRET"
  exit 1
fi

echo "Running topic checkpoint against: $BOOTSTRAP_SERVERS"
gradle run --args='--check-only'
