#!/usr/bin/env bash
# Provisions MinIO for EMF: creates buckets and an application user with read/write access.
# Idempotent - safe to run on every "compose up".
# Note: the MinIO image is minimal (no grep/sed), so only bash builtins and coreutils are used.
set -euo pipefail

INIT_DIR="$(cd "$(dirname "$0")" && pwd)"
ALIAS=emf

log() { echo "[minio-init] $*"; }

log "Waiting for MinIO at ${MINIO_URL}"
# Wait up to 5 minutes - containers may be started all at once (e.g. Podman Desktop group start),
# without compose waiting for the service healthcheck first
for attempt in $(seq 1 150); do
    if mc alias set "${ALIAS}" "${MINIO_URL}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" > /dev/null 2>&1 \
        && mc admin info "${ALIAS}" > /dev/null 2>&1; then
        break
    fi
    if [ "${attempt}" -eq 150 ]; then
        log "MinIO did not become available"
        exit 1
    fi
    sleep 2
done

log "Creating buckets"
while read -r bucket || [ -n "${bucket}" ]; do
    bucket="${bucket%$'\r'}"
    if [ -z "${bucket}" ] || [[ "${bucket}" == \#* ]]; then
        continue
    fi
    mc mb --ignore-existing "${ALIAS}/${bucket}"
done < "${INIT_DIR}/buckets.txt"

log "Creating application user '${MINIO_EMF_USER}'"
mc admin user add "${ALIAS}" "${MINIO_EMF_USER}" "${MINIO_EMF_PASSWORD}"
# Built-in "readwrite" policy; attaching an already attached policy is reported as an error, which is fine here
if ! output="$(mc admin policy attach "${ALIAS}" readwrite --user "${MINIO_EMF_USER}" 2>&1)"; then
    if [[ "${output,,}" == *already* ]]; then
        log "Policy 'readwrite' already attached to '${MINIO_EMF_USER}'"
    else
        echo "${output}"
        exit 1
    fi
fi

log "Provisioned buckets:"
mc ls "${ALIAS}"
log "Done"
