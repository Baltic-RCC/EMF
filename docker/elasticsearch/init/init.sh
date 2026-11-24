#!/usr/bin/env bash
# Provisions Elasticsearch for EMF: ingest pipelines, index templates, reference indices,
# kibana_system password and an API key of the elastic superuser for EMF tools, which authenticate
# with API keys only (written to ${SECRETS_DIR}/elasticsearch_api_key).
# Idempotent - safe to run on every "compose up"; an existing valid API key is reused.
set -euo pipefail

INIT_DIR="$(cd "$(dirname "$0")" && pwd)"
API_KEY_FILE="${SECRETS_DIR}/elasticsearch_api_key"

log() { echo "[elasticsearch-init] $*"; }

# es <METHOD> <PATH> [curl args...] - call Elasticsearch as the elastic superuser, fail on HTTP errors
es() {
    local method="$1" path="$2"
    shift 2
    # "|| return" keeps curl's exit code - the trailing echo would otherwise mask failures
    curl -sS --fail-with-body -u "elastic:${ELASTIC_PASSWORD}" -H "Content-Type: application/json" \
        -X "${method}" "${ELASTICSEARCH_URL}${path}" "$@" || return
    echo
}

# put_each <API PATH PREFIX> <DIR> - PUT every <name>.json in DIR to <PREFIX>/<name> (empty DIR is skipped)
put_each() {
    local prefix="$1" dir="$2" file name
    for file in "${dir}"/*.json; do
        [ -e "${file}" ] || continue
        name="$(basename "${file}" .json)"
        log "PUT ${prefix}/${name}"
        es PUT "${prefix}/${name}" --data-binary "@${file}" > /dev/null
    done
}

log "Waiting for cluster at ${ELASTICSEARCH_URL}"
# Wait up to 5 minutes - containers may be started all at once (e.g. Podman Desktop group start),
# without compose waiting for the service healthcheck first
for attempt in $(seq 1 150); do
    if es GET "/_cluster/health?wait_for_status=yellow&timeout=5s" > /dev/null 2>&1; then
        break
    fi
    if [ "${attempt}" -eq 150 ]; then
        log "Elasticsearch did not become available"
        exit 1
    fi
    sleep 2
done

# Pipelines first, so index templates can reference them via "index.default_pipeline"
log "Creating ingest pipelines"
put_each "/_ingest/pipeline" "${INIT_DIR}/ingest-pipelines"

log "Creating index templates"
put_each "/_index_template" "${INIT_DIR}/index-templates"

log "Creating reference indices"
tr -d '\r' < "${INIT_DIR}/reference-indices.txt" | grep -Ev '^\s*(#|$)' | while read -r index; do
    status="$(curl -s -o /dev/null -w '%{http_code}' -u "elastic:${ELASTIC_PASSWORD}" -I "${ELASTICSEARCH_URL}/${index}")"
    if [ "${status}" = "200" ]; then
        log "Index ${index} already exists"
    else
        log "PUT /${index}"
        es PUT "/${index}" > /dev/null
    fi
done

log "Setting password of built-in user 'kibana_system' (used by Kibana to connect)"
es POST "/_security/user/kibana_system/_password" --data-binary "{\"password\": \"${KIBANA_SYSTEM_PASSWORD}\"}" > /dev/null

log "Ensuring API key '${ELASTICSEARCH_API_KEY_NAME}' for EMF tools"
mkdir -p "${SECRETS_DIR}"
api_key=""
if [ -s "${API_KEY_FILE}" ]; then
    existing_key="$(tr -d '\r\n' < "${API_KEY_FILE}")"
    if curl -sf -o /dev/null -H "Authorization: ApiKey ${existing_key}" "${ELASTICSEARCH_URL}/_security/_authenticate"; then
        log "Existing API key in ${API_KEY_FILE} is valid, reusing it"
        api_key="${existing_key}"
    else
        log "Existing API key in ${API_KEY_FILE} is not valid (data volume recreated?), creating a new one"
    fi
fi
if [ -z "${api_key}" ]; then
    # No role descriptors - the key inherits the full (superuser) privileges of the elastic user
    response="$(es POST "/_security/api_key" --data-binary "{\"name\": \"${ELASTICSEARCH_API_KEY_NAME}\"}")"
    api_key="$(echo "${response}" | sed -n 's/.*"encoded" *: *"\([^"]*\)".*/\1/p')"
    if [ -z "${api_key}" ]; then
        log "Could not parse API key from response: ${response}"
        exit 1
    fi
    printf '%s\n' "${api_key}" > "${API_KEY_FILE}"
    # Best effort - not supported on every bind-mounted filesystem (e.g. Windows drives)
    chmod 600 "${API_KEY_FILE}" || true
    log "New API key written to ${API_KEY_FILE}"
fi

log "Provisioned index templates:"
es GET "/_cat/templates/emfos*?v&s=name"
log "Provisioned indices:"
es GET "/_cat/indices?v&s=index&expand_wildcards=open"
log "Done - use the key from docker/.secrets/elasticsearch_api_key as ELK_TOKEN"
