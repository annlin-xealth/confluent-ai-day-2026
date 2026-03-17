#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <connect-rest-url> <connector-config.json>"
  echo "Example: $0 http://localhost:8083 connectors/upstream-to-heart-rate-raw.template.json"
  exit 1
fi

CONNECT_URL="$1"
CONFIG_FILE="$2"

curl -sS -X POST "${CONNECT_URL%/}/connectors" \
  -H "Content-Type: application/json" \
  --data @"$CONFIG_FILE"

echo
