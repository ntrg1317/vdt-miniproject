#!/bin/bash

echo "==> Waiting for Debezium Connect to be ready..."
sleep 10

curl -X POST \
  -H "Accept:application/json" \
  -H "Content-Type:application/json" \
  --data @config/postgresql/postgres_source.json \
  http://debezium-connect:8083/connectors

echo "==> Debezium connector registered."
