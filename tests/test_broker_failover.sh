#!/usr/bin/env bash

set -euo pipefail


BROKER_CONTAINER="${BROKER_CONTAINER:-edge-telemetry-platform-mosquitto-1}"

HTTP_HOST="${HTTP_HOST:-localhost}"
HTTP_PORT="${HTTP_PORT:-8080}"

HEALTH_PATH="${HTTP_ROUTE_HEALTH:-/health}"
METRICS_PATH="${HTTP_ROUTE_METRICS:-/metrics}"

HEALTH_URL="http://${HTTP_HOST}:${HTTP_PORT}${HEALTH_PATH}"
METRICS_URL="http://${HTTP_HOST}:${HTTP_PORT}${METRICS_PATH}"

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
    local ts health metrics status mqtt queue pending fails p95

    ts=$(date +"%H:%M:%S")

    health=$(curl -s "$HEALTH_URL")
    metrics=$(curl -s "$METRICS_URL")

    status=$(echo "$health" | jq -r '.status')
    mqtt=$(echo "$health" | jq -r '.mqtt_connected')
    queue=$(echo "$health" | jq -r '.queue_size')
    pending=$(echo "$health" | jq -r '.sql_pending')

    fails=$(echo "$metrics" | jq -r '.cont_fails')
    p95=$(echo "$metrics" | jq -r '.latency_p95')

    printf "[%s] status=%-10s mqtt=%-5s queue=%-6s pending=%-8s fails=%-8s p95=%s\n" \
        "$ts" "$status" "$mqtt" "$queue" "$pending" "$fails" "$p95"
}

wait_until_recovered() {
    echo_header "Waiting for full recovery"

    local start end duration health status pending queue

    start=$(date +%s)

    while true; do
        print_status

        health=$(curl -s "$HEALTH_URL")

        status=$(echo "$health" | jq -r '.status')
        pending=$(echo "$health" | jq -r '.sql_pending')
        queue=$(echo "$health" | jq -r '.queue_size')

        if [[ "$status" == "healthy" && "$pending" == "0" && "$queue" == "0" ]]; then
            end=$(date +%s)
            duration=$((end - start))

            echo
            echo "Recovery complete in ${duration}s"
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

echo_header "Stopping MQTT broker"

docker stop "$BROKER_CONTAINER"

echo
echo "Broker offline for ${OUTAGE_SECONDS}s"
echo

for _ in $(seq 1 "$OUTAGE_SECONDS"); do
    print_status
    sleep "$POLL_INTERVAL"
done

echo_header "Restarting MQTT broker"

docker start "$BROKER_CONTAINER"

wait_until_recovered

echo_header "Final system state"

for _ in {1..5}; do
    print_status
    sleep "$POLL_INTERVAL"
done

echo
echo "Failover validation completed successfully"