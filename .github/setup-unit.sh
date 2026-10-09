#!/usr/bin/env bash
# .github/setup-unit.sh
#
# Starts the services required for plugin-iceberg integration tests.
# This script is invoked by the Kestra shared CI workflow before running
# `./gradlew test`. It must be idempotent (safe to run multiple times)
# and executable both locally and in GitHub Actions.
#
# Services started (via docker-compose-ci.yml):
#   - minio : S3 storage backend (port 9000)
#   - mc    : MinIO client initializing the 'warehouse' bucket
#   - rest  : Apache Iceberg REST catalog fixture (port 8181)
#
# Usage:
#   bash .github/setup-unit.sh
set -euo pipefail

compose_file="docker-compose-ci.yml"

# ── Tear down any previous run so we start with a clean state ───────────────
echo "[setup-unit] Resetting Iceberg REST and MinIO containers..."
docker compose -f "${compose_file}" down -v --remove-orphans >/dev/null 2>&1 || true

# ── Start services ───────────────────────────────────────────────────────────
echo "[setup-unit] Starting Iceberg REST catalog and MinIO fixture..."
docker compose -f "${compose_file}" up -d --wait

# ── Ensure the warehouse bucket exists ───────────────────────────────────────
echo "[setup-unit] Ensuring 'warehouse' bucket exists in MinIO..."
docker compose -f "${compose_file}" run --rm --entrypoint /bin/sh mc -c "
  until (/usr/bin/mc alias set myminio http://minio:9000 admin password); do
    sleep 1;
  done;
  /usr/bin/mc mb --ignore-existing myminio/warehouse || true;
" >/dev/null 2>&1 || true

# ── Verify the Iceberg REST catalog is reachable ─────────────────────────────
echo "[setup-unit] Verifying Iceberg REST catalog endpoint..."
for attempt in $(seq 1 30); do
    if curl -sf "http://localhost:8181/v1/config" >/dev/null 2>&1; then
        echo "[setup-unit] Iceberg REST catalog is ready."
        break
    fi
    if [ "${attempt}" -eq 30 ]; then
        echo "[setup-unit] Iceberg REST catalog did not become ready in time." >&2
        docker compose -f "${compose_file}" logs --no-color --tail=40 rest >&2 || true
        exit 1
    fi
    echo "[setup-unit] REST catalog not ready yet (attempt ${attempt}/30), retrying in 2s..."
    sleep 2
done

# ── Enable integration tests in subsequent CI workflow steps ─────────────────
if [ -n "${GITHUB_ENV:-}" ]; then
    echo "ICEBERG_REST_INTEGRATION=true" >> "${GITHUB_ENV}"
    echo "[setup-unit] Exported ICEBERG_REST_INTEGRATION=true to GITHUB_ENV."
fi

echo "[setup-unit] All services are up. Ready to run tests."
