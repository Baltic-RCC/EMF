#!/usr/bin/env bash
# Provisions Kibana for EMF: creates data views for EMF indices.
# Idempotent - safe to run on every "compose up"; data views are overwritten by fixed id.
set -euo pipefail

INIT_DIR="$(cd "$(dirname "$0")" && pwd)"

log() { echo "[kibana-init] $*"; }

# kbn <METHOD> <PATH> [curl args...] - call Kibana API as the elastic superuser, fail on HTTP errors
kbn() {
    local method="$1" path="$2"
    shift 2
    # "|| return" keeps curl's exit code - the trailing echo would otherwise mask failures
    curl -sS --fail-with-body -u "elastic:${ELASTIC_PASSWORD}" -H "Content-Type: application/json" \
        -H "kbn-xsrf: true" -X "${method}" "${KIBANA_URL}${path}" "$@" || return
    echo
}

log "Waiting for Kibana at ${KIBANA_URL}"
# Wait up to 5 minutes - containers may be started all at once (e.g. Podman Desktop group start),
# without compose waiting for the service healthcheck first
for attempt in $(seq 1 150); do
    if kbn GET "/api/status" 2> /dev/null | grep -q '"level":"available"'; then
        break
    fi
    if [ "${attempt}" -eq 150 ]; then
        log "Kibana did not become available"
        exit 1
    fi
    sleep 2
done

log "Creating data views"
tr -d '\r' < "${INIT_DIR}/data-views.txt" | grep -Ev '^\s*(#|$)' | while IFS='|' read -r pattern name; do
    # Stable id derived from the pattern, e.g. "emfos-logs*" -> "emfos-logs"
    id="${pattern//\*/}"
    log "Data view '${name}' (${pattern})"
    kbn POST "/api/data_views/data_view" --data-binary "{
        \"override\": true,
        \"data_view\": {\"id\": \"${id}\", \"title\": \"${pattern}\", \"name\": \"${name}\", \"timeFieldName\": \"@timestamp\"}
    }" > /dev/null
done

log "Setting default data view to 'EMF logs'"
kbn POST "/api/data_views/default" --data-binary '{"data_view_id": "emfos-logs", "force": true}' > /dev/null

log "Done - open ${KIBANA_URL/kibana/localhost} and log in as 'elastic'"
