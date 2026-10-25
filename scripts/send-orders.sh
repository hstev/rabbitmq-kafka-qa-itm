#!/usr/bin/env bash
# Envia N pedidos a POST /orders y muestra respuesta + tiempo de respuesta.
# Uso: ./scripts/send-orders.sh [rabbitmq|kafka] [cantidad] [url]
#   ./scripts/send-orders.sh rabbitmq 5
#   ./scripts/send-orders.sh kafka 10 http://localhost:3000/orders
set -euo pipefail

ARCH="${1:-rabbitmq}"
COUNT="${2:-1}"
URL="${3:-http://localhost:3000/orders}"
CUSTOMERS=(Pedro Carlos Ana Lucia Mateo Sofia)

for i in $(seq 1 "$COUNT"); do
  customer="${CUSTOMERS[$(( (i - 1) % ${#CUSTOMERS[@]} ))]}"
  if [[ "$ARCH" == "kafka" ]]; then
    body="{\"customerName\":\"$customer\",\"product\":\"Libro de arquitectura\",\"quantity\":$i,\"unitPrice\":85000}"
  else
    body="{\"customerName\":\"$customer\",\"product\":\"Libro de arquitectura\",\"quantity\":$i}"
  fi
  printf '\n[%s] Pedido %d/%d -> %s\n' "$(date +%T)" "$i" "$COUNT" "$body"
  curl -s -X POST "$URL" \
    -H 'Content-Type: application/json' \
    -H "X-Correlation-Id: corr-qa-$(date +%s)-$i" \
    -d "$body" \
    -w '\nHTTP %{http_code} | tiempo total: %{time_total}s\n'
done
