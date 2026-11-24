#!/usr/bin/env bash
# Provisions RabbitMQ for EMF: imports every *.def.json in ${DEFINITIONS_DIR} (in name order) and creates the
# application user.
# Idempotent - safe to run on every "compose up".
set -euo pipefail

export RABBITMQADMIN_NON_INTERACTIVE_MODE=true

log() { echo "[rabbitmq-init] $*"; }

rmq() {
    rabbitmqadmin --host "${RABBITMQ_HOST}" --port "${RABBITMQ_MANAGEMENT_PORT}" \
        --username "${RABBITMQ_ADMIN_USER}" --password "${RABBITMQ_ADMIN_PASSWORD}" "$@"
}

log "Waiting for management API at ${RABBITMQ_HOST}:${RABBITMQ_MANAGEMENT_PORT}"
# Wait up to 5 minutes - containers may be started all at once (e.g. Podman Desktop group start),
# without compose waiting for the service healthcheck first
for attempt in $(seq 1 150); do
    if rmq show overview > /dev/null 2>&1; then
        break
    fi
    if [ "${attempt}" -eq 150 ]; then
        log "RabbitMQ management API did not become available"
        exit 1
    fi
    sleep 2
done

# Name order matters when a file refers to objects of another, e.g. shovels in infra-shovel.def.json publish to
# exchanges defined in emfos.def.json
for definitions_file in "${DEFINITIONS_DIR}"/*.def.json; do
    [ -e "${definitions_file}" ] || continue
    log "Importing definitions from ${definitions_file}"
    rmq definitions import --file "${definitions_file}"
done

log "Creating application user '${RABBITMQ_EMF_USER}'"
rmq users declare --name "${RABBITMQ_EMF_USER}" --password "${RABBITMQ_EMF_PASSWORD}"
rmq permissions declare --vhost / --username "${RABBITMQ_EMF_USER}" --configure '.*' --write '.*' --read '.*'

log "Provisioned exchanges:"
rmq list exchanges
log "Provisioned queues:"
rmq list queues
log "Provisioned bindings:"
rmq list bindings
log "Provisioned parameters (shovels):"
rmq list parameters
log "Done"
