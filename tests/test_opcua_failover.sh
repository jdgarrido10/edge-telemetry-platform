#!/usr/bin/env bash

set -euo pipefail

OPCUA_CONTAINER="${OPCUA_CONTAINER:-opc-ua-sim}"

HTTP_HOST="${HTTP_HOST:-localhost}"
HTTP_PORT="${HTTP_PORT:-8080}"
HEALTH_URL="http://${HTTP_HOST}:${HTTP_PORT}/health"

OUTAGE_SECONDS="${OUTAGE_SECONDS:-15}"
POLL_INTERVAL="${POLL_INTERVAL:-1}"

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

echo_header() {
    echo
    echo "=============================================================="
    echo "$1"
    echo "=============================================================="
}

print_status() {
    local ts health opcua status queue mqtt

    ts=$(date +"%H:%M:%S")

    health=$(curl -s "$HEALTH_URL")

    status=$(echo "$health" | jq -r '.status')
    mqtt=$(echo "$health" | jq -r '.mqtt_connected')
    opcua=$(echo "$health" | jq -r '.opc_ua_connected')
    queue=$(echo "$health" | jq -r '.queue_size')

    printf "[%s] status=%-10s mqtt=%-5s opcua=%-5s queue=%-6s\n" \
        "$ts" "$status" "$mqtt" "$opcua" "$queue"
}

wait_for_recovery() {
    echo_header "Waiting for OPC UA recovery"

    local start end duration health opcua mqtt queue status

    start=$(date +%s)

    while true; do
        print_status

        health=$(curl -s "$HEALTH_URL")

        status=$(echo "$health" | jq -r '.status')
        opcua=$(echo "$health" | jq -r '.opc_ua_connected')
        mqtt=$(echo "$health" | jq -r '.mqtt_connected')
        queue=$(echo "$health" | jq -r '.queue_size')

        if [[ "$opcua" == "true" && "$status" == "healthy" ]]; then
            end=$(date +%s)
            duration=$((end - start))

            echo
            echo "OPC UA recovery completed in ${duration}s"
            break
        fi

        sleep "$POLL_INTERVAL"
    done
}

# ─────────────────────────────────────────────────────────────
# Test flow
# ─────────────────────────────────────────────────────────────

echo_header "Initial system state"

for _ in {1..5}; do
    print_status
    sleep "$POLL_INTERVAL"
done

echo_header "Stopping OPC UA simulator"

docker stop "$OPCUA_CONTAINER"

echo
echo "OPC UA offline for ${OUTAGE_SECONDS}s"
echo

for _ in $(seq 1 "$OUTAGE_SECONDS"); do
    print_status
    sleep "$POLL_INTERVAL"
done

echo_header "Restarting OPC UA simulator"

docker start "$OPCUA_CONTAINER"

wait_for_recovery

echo_header "Final system state"

for _ in {1..5}; do
    print_status
    sleep "$POLL_INTERVAL"
done

echo
echo "OPC UA failover validation completed successfully"