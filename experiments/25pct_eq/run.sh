#!/usr/bin/env bash
set -euo pipefail

EXPERIMENT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$EXPERIMENT_DIR/../.."
JAVA_DIR="$ROOT/java-publication"
PYTHON_DIR="$ROOT/python-network"
SCENARIO="$(basename "$EXPERIMENT_DIR")"

BROKER_PIDS=""
SUBSCRIBER_PIDS=""

cleanup() {
    echo "Cleanup..."
    for pid in $BROKER_PIDS $SUBSCRIBER_PIDS; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    docker compose -f "$ROOT/docker-compose.yml" down 2>/dev/null || true
}
trap cleanup EXIT SIGINT SIGTERM

echo "=== Experiment: $SCENARIO ==="

echo "--- 1. Starting Kafka ---"
docker compose -f "$ROOT/docker-compose.yml" up -d
sleep 5

echo "--- 2. Generating data (Java) ---"
cd "$JAVA_DIR"
./gradlew run --args="-c $EXPERIMENT_DIR/input.json -o $EXPERIMENT_DIR" 2>&1 | tail -5

echo "--- 3. Starting brokers ---"
cd "$PYTHON_DIR"
for bid in broker_0 broker_1 broker_2; do
    uv run broker.py --id "$bid" &
    BROKER_PIDS="$BROKER_PIDS $!"
    sleep 1
done
sleep 3

echo "--- 4. Starting subscribers ---"
for sid in client_1 client_2 client_3; do
    cd "$EXPERIMENT_DIR"
    uv run --project "$PYTHON_DIR" "$PYTHON_DIR/subscriber.py" --id "$sid" \
        --subscriptions "$EXPERIMENT_DIR/subscriptions.txt" \
        --verify-matches &
    SUBSCRIBER_PIDS="$SUBSCRIBER_PIDS $!"
    sleep 1
done
sleep 3

echo "--- 5. Running publisher (3 minutes) ---"
cd "$EXPERIMENT_DIR"
uv run --project "$PYTHON_DIR" "$PYTHON_DIR/publisher.py" \
    --publications "$EXPERIMENT_DIR/publications.txt" \
    --duration 3

echo "--- 6. Aggregating results ---"
python3 "$EXPERIMENT_DIR/aggregate.py"
