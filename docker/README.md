# Docker containers
Build the base container:
````
bash run.sh
````
Build the container cluster in detached mode:
````
docker compose up -d
````
Shut down compose:
````
docker compose down
````
Stop container:
````
docker stop container_name
````
Attach to a container:
````
docker exec container_name bash
````