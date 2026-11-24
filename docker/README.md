# Docker

```
docker/
├── compose.yaml               # dependencies: RabbitMQ, Elasticsearch, Kibana, MinIO (project "emf")
├── compose.services.yaml      # EMF services/workers (project "emf-services")
├── scripts/
│   └── run_task_generator_manual.py  # manual task generation against the local stack
├── .env.example               # all tunables with their defaults - copy to .env to override
├── Dockerfile                 # EMF base and worker images
├── rabbitmq/
│   ├── rabbitmq.conf          # broker settings (mounted to /etc/rabbitmq/conf.d/)
│   ├── enabled_plugins        # management, prometheus, shovel
│   ├── emfos.def.json         # definitions: exchanges, queues, bindings
│   ├── infra-shovel.def.json  # definitions: shovels from OPDE EDX (AMQP 1.0) into the local broker
│   └── init/                  # rabbitmq-init: imports *.def.json + app user
├── elasticsearch/
│   └── init/                  # elasticsearch-init: ingest pipelines, index templates, reference indices, kibana_system, API key
├── kibana/
│   └── init/                  # kibana-init: data views for EMF indices (data-views.txt)
├── minio/
│   └── init/                  # minio-init: buckets.txt + app user
└── .secrets/                  # generated credentials (git-ignored), e.g. elasticsearch_api_key
```

## Local environment

The environment is split into two compose projects, which start and stop independently:

- `compose.yaml` (project `emf`) - the infrastructure EMF tools depend on, kept running
- `compose.services.yaml` (project `emf-services`) - EMF workers, see [EMF services](#emf-services)

`compose.yaml` starts the infrastructure EMF tools depend on. Each service has an init container that
provisions its initial configuration once the service is healthy, then exits with code 0. Init containers
are idempotent and run again on every `up`.

| Service       | Image                                       | Endpoints                                                  |
|---------------|---------------------------------------------|------------------------------------------------------------|
| RabbitMQ      | `rabbitmq:4.2.7-management-alpine`          | AMQP `localhost:5672`, UI http://localhost:15672           |
| Elasticsearch | `elasticsearch:8.19.25` (basic license)     | http://localhost:9200 (security on, plain HTTP)            |
| Kibana        | `kibana:8.19.25` (basic license)            | UI http://localhost:5601                                   |
| MinIO         | `chainguard/minio:latest` (AGPL community)  | S3 API http://localhost:9000, console http://localhost:9001 |

Ports bind to `127.0.0.1` only by default (`BIND_ADDRESS` in `.env`).

### Usage

Run from the `docker/` folder (`podman compose` and `docker compose` both work):

```
podman compose up -d            # start and provision
podman compose ps -a            # *-init containers should show "Exited (0)"
podman compose logs rabbitmq-init elasticsearch-init kibana-init minio-init
podman compose down             # stop, keep data
podman compose down -v          # stop and delete data volumes
```

### Podman Desktop

To start and stop the stack with buttons in Podman Desktop, create the containers once without starting them:

```
podman compose create
```

The containers then appear as the `emf` group under *Containers*, with start/stop buttons for the whole group.
Podman Desktop starts all containers at the same time, without waiting on the healthchecks the way `compose up`
does - the init containers handle that by retrying for up to 5 minutes until their service is ready. The full stack
is usually up in about 2 minutes. After a `podman compose down`, run `podman compose create` again.

### Default credentials (local development only)

| Service       | Admin                               | Application (EMF tools)                                 |
|---------------|-------------------------------------|---------------------------------------------------------|
| RabbitMQ      | `admin` / `admin`                   | `emf` / `emf` (full access on vhost `/`)                |
| Elasticsearch | `elastic` / `elastic`               | `elastic` API key in `docker/.secrets/elasticsearch_api_key` |
| Kibana        | `elastic` / `elastic`               | - (Kibana itself connects as `kibana_system`)           |
| MinIO         | `admin` / `password`                | `emf` / `password` (built-in `readwrite` policy)        |

### What gets provisioned

**RabbitMQ** - every `rabbitmq/*.def.json` (RabbitMQ definitions format), imported as is in name order:

`emfos.def.json`:

- headers exchanges `emf.merge.tasks` (alternate exchange `dev.null`), `opde-from.edx` and `dev.null`,
  which collects dead-lettered and unroutable messages into `dead.letter.emf.tasks` / `dead.letter.emf.models`
- quorum queues `emf.merge.tasks.*`, `emf.object-storage.models.opdm`, `emf.validation.models.opdm`
  and the two dead-letter queues

`infra-shovel.def.json` - dynamic shovels from the OPDE EDX broker (AMQP 1.0), whose `src-uri` is a placeholder
(`amqp://opde-edx-host.example:6672`) - set the real broker URI locally, do not commit it. Until then the shovels
keep retrying every 10 s, which only shows in the broker log and the *Shovel Status* page of the management UI:
- `opde-edx.to.rabbit`: `edx.endpoint.inbox.rabbit` → exchange `opde-from.edx`
- `opde-edx.reply.to.rabbit`: `edx.endpoint.reply.rabbit` → queue `opde-to.edx.reply`

Also:
- `consumer_timeout` raised to 2h in `rabbitmq.conf`, since single-message consumers ack only after a task finishes

Every exchange a binding refers to must be defined in the file, otherwise the whole import is rejected
(`resources_missing`). Changing the type of an existing exchange or queue (e.g. classic → quorum) is not
possible in place - delete it in the management UI, or reset with `podman compose down -v`.

**Elasticsearch** - `elasticsearch/init/`:
- ingest pipelines from `ingest-pipelines/<name>.json` (none yet) - each file is the body of
  `PUT _ingest/pipeline/<name>`; reference one from an index template with `"index.default_pipeline": "<name>"`
- index templates `emfos-igm-validation`, `emfos-logs`, `emfos-merge-reports`, `emfos-opde-models`,
  `emfos-schedules`, `emfos-tasks`, each matching `<name>*` (EMF writes monthly `<name>-YYYYMM` indices):
  1 shard, 0 replicas, `@timestamp` as date. No ILM - indices are kept until deleted
- empty reference indices from `reference-indices.txt` (EMF queries them by exact name)
- no custom roles or users - everything runs as the `elastic` superuser. EMF tools authenticate with API keys
  only, so an unrestricted API key of the `elastic` user is written to `docker/.secrets/elasticsearch_api_key`;
  the key is reused while it stays valid and recreated if the data volume was removed
- password of the built-in `kibana_system` user, which Kibana uses to connect (`KIBANA_SYSTEM_PASSWORD`)

**Kibana** - `kibana/init/data-views.txt`: one data view per EMF index family (`emfos-logs*`, `emfos-tasks*`, ...)
with `@timestamp` as time field; `EMF logs` is the default. Kibana starts only after `elasticsearch-init` succeeded.

**MinIO** - `minio/init/`: buckets from `buckets.txt` and the `emf` user.

To change provisioning, edit the files under `*/init/` and run `podman compose up -d` again.

## EMF services

`compose.services.yaml` runs EMF workers against the dependencies. Start the dependencies first: the workers
join their `emf` network (declared `external`) and read the Elasticsearch API key written by `elasticsearch-init`.

| Service           | Image                                         | Consumes                          | Publishes to                 |
|-------------------|-----------------------------------------------|-----------------------------------|------------------------------|
| `model-retriever` | `balticrcc/emfos-model-retriever:latest`      | `emf.object-storage.models.opdm`  | `emf.validation.models.opdm` |
| `model-validator` | `balticrcc/emfos-model-validator:latest`      | `emf.validation.models.opdm`      | - (Elasticsearch, MinIO)     |
| `model-merger`    | `balticrcc/emfos-model-merger:latest`         | `emf.merge.tasks.cgm`             | - (Elasticsearch, MinIO)     |

```
podman compose up -d                                      # dependencies first
podman compose -f compose.services.yaml up -d             # workers
podman compose -f compose.services.yaml logs -f model-retriever
podman compose -f compose.services.yaml down              # dependencies keep running
```

- **Images** come from Docker Hub (override with `MODEL_*_IMAGE` in `.env`). The local `emf/` and `config/`
  are mounted over the copies in the image, so code changes apply on restart without a rebuild.
- **Startup**: the entrypoint waits for `.secrets/elasticsearch_api_key` and exports it as `ELK_TOKEN`. A worker
  started before RabbitMQ is ready exits and is restarted (`restart: unless-stopped`).
- **OPDM** is not part of the local stack. Model retriever connects to OPDM on the first message, so it starts
  consuming without it; messages fail (and are dead-lettered after 3 deliveries) until `OPDM_SERVER`,
  `OPDM_USERNAME` and `OPDM_PASSWORD` point to a reachable OPDM.
- **No forwarding**: validator and merger only store results (Elasticsearch, MinIO). `OUTPUT_RMQ_EXCHANGE=None`
  disables publishing the handled message to a next-stage exchange (supported by both RabbitMQ consumers).
- **Consumer type**: validator and merger run as `LONG_LIVING` consumers; the default `SINGLE_MESSAGE` handles one
  message and exits (meant for KEDA jobs), which would make the container restart continuously.
- **Start a subset** to save memory, e.g. `podman compose -f compose.services.yaml up -d model-validator`.
- **Podman Desktop** shows the workers as a separate `emf-services` group.

### Generating tasks manually

`scripts/run_task_generator_manual.py` runs the task generator in manual mode from the project virtualenv and
publishes the tasks to `emf.merge.tasks`, which routes them by `process_id` to the merger queues
(`CGM_CREATION` → `emf.merge.tasks.cgm`, `RMM_CREATION` → `emf.merge.tasks.rmm`). Set the parameters from
`config/task_generator/task_generator.properties` in `TASK_PARAMETERS` at the top of the script, then from the
repository root:

```
.venv\Scripts\python.exe docker\scripts\run_task_generator_manual.py --list   # available RUN_TYPE values
.venv\Scripts\python.exe docker\scripts\run_task_generator_manual.py          # generate and publish
```

A task the merger fails on is redelivered up to 3 times, then dead-lettered to `dead.letter.emf.tasks`.

### Running a worker outside containers

Settings in `config/**/*.properties` can be overridden with environment variables of the same name. From the host
(e.g. an IDE) the dependencies are reached through the published ports:

```
RMQ_SERVER=localhost  RMQ_PORT=5672  RMQ_USERNAME=emf  RMQ_PASSWORD=emf
ELK_SERVER=http://localhost:9200  ELK_TOKEN=<docker/.secrets/elasticsearch_api_key>  ELK_SSL_VERIFY=False
MINIO_SERVER=localhost:9000  MINIO_USERNAME=emf  MINIO_PASSWORD=password  MINIO_AUTH_METHOD=STATIC  MINIO_SECURE=False
```

`MINIO_AUTH_METHOD=STATIC` uses the username/password directly as access/secret key; the default `LDAP` gets
temporary credentials through STS (`AssumeRoleWithLDAPIdentity`) over HTTPS, which the local MinIO does not provide.

### Notes

- **MinIO image**: MinIO no longer publishes community images (`minio/minio` on Docker Hub and quay.io are
  not pullable). Chainguard rebuilds the AGPL source; its free tier only offers `latest`, so pin a digest via
  `MINIO_IMAGE` in `.env` if you need a reproducible version.
- **Changing passwords**: RabbitMQ and Elasticsearch take the admin password only when their data volume is
  first created, so after changing it run `podman compose down -v`. Minimum lengths: Elasticsearch 6,
  MinIO 8 characters.
- **Memory**: Elasticsearch heap defaults to 512m (`ELASTICSEARCH_HEAP`) and the container uses ~1.1 GB in
  total; Kibana adds roughly another 0.5-0.7 GB.
- **Compose hangs with Podman**: `podman compose` runs `docker-compose`, which talks to the Docker API pipe. If
  Docker Desktop is installed, the default `docker_engine` pipe may belong to it and hang. Point it to Podman:
  `$env:DOCKER_HOST = "npipe:////./pipe/podman-machine-default"` (PowerShell) or
  `export DOCKER_HOST=npipe:////./pipe/podman-machine-default` (Git Bash).
- **Windows**: files in `docker/` are forced to LF line endings by `docker/.gitattributes`, because they are
  mounted into Linux containers.

## Worker images

`Dockerfile` defines the `emfos-base` image and one target per worker (same as CI). From the repository root:

```
docker build . -f docker/Dockerfile --target emfos-base -t emfos-base:latest
docker build . -f docker/Dockerfile --target emfos-model-validator --build-arg BASE_IMAGE=emfos-base:latest -t emfos-model-validator:latest
```
