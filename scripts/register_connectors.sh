#!/usr/bin/env bash
set -e

CONNECT_URL=${CONNECT_URL:-http://localhost:8083}

echo "Registering source connector cbr-pg-raw-source..."
curl -s -X PUT \
  -H "Content-Type: application/json" \
  "${CONNECT_URL}/connectors/cbr-pg-raw-source/config" \
  -d @connectors/cbr-pg-raw-source.json
echo
echo "Done."

echo "Registering sink connector cbr-pg-dwh-sink..."
curl -s -X PUT \
  -H "Content-Type: application/json" \
  "${CONNECT_URL}/connectors/cbr-pg-dwh-sink/config" \
  -d @connectors/cbr-pg-dwh-sink.json
echo
echo "Done."
