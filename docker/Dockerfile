# Global argument for base image for workers, can be overridden when building with --build-arg
ARG BASE_IMAGE=balticrcc/emfos-base:latest

# Define base container image (platform has to be defined if using with ARM64 architecture)
FROM python:3.11-slim as emfos-base
#FROM --platform=linux/amd64 python:3.11-slim as emfos-base

# Set working directory
WORKDIR /src

# Install tools to set up python environment
RUN apt-get update && apt-get install -y --no-install-recommends procps && rm -rf /var/lib/apt/lists/*
RUN pip install --upgrade setuptools pip uv

# Sync python modules as dependencies
COPY pyproject.toml .
COPY uv.lock .
RUN uv export -o pylock.toml
RUN uv pip sync pylock.toml --system  && uv cache clean

# Copy license file and dependencies license reference
RUN mkdir -p /licenses
COPY LICENSE /licenses/
COPY licenses/ /licenses/

# Set to use interpreter from venv (if venv is used)
#ENV PATH="/.venv/bin:$PATH"

FROM ${BASE_IMAGE} as emfos-model-retriever

# Set working directory
WORKDIR /src

# Copy files to container
COPY emf/model_retriever/ ./emf/model_retriever/
COPY emf/common/ ./emf/common/
COPY config/ ./config/

# Start process
ENV PYTHONPATH="${PYTHONPATH}:/src"
ENTRYPOINT ["python", "emf/model_retriever/opdm_worker.py"]
#ENTRYPOINT ["/bin/bash"]

FROM ${BASE_IMAGE} as emfos-local-model-retriever

# Set working directory
WORKDIR /src

# Copy files to container
COPY emf/model_retriever/ ./emf/model_retriever/
COPY emf/common/ ./emf/common/
COPY config/ ./config/

# Start process
ENV PYTHONPATH="${PYTHONPATH}:/src"
ENTRYPOINT ["python", "emf/model_retriever/local_worker.py"]

FROM ${BASE_IMAGE} as emfos-model-validator

# Set working directory
WORKDIR /src

# Copy files to container
COPY emf/model_validator/ ./emf/model_validator/
COPY emf/common/ ./emf/common/
COPY config/ ./config/

# Start process
ENV PYTHONPATH="${PYTHONPATH}:/src"
ENTRYPOINT ["python", "emf/model_validator/worker.py"]
#ENTRYPOINT ["/bin/bash"]

FROM ${BASE_IMAGE} as emfos-model-merger

# Set working directory
WORKDIR /src

# Copy files to container
COPY emf/model_merger/ ./emf/model_merger/
COPY emf/common/ ./emf/common/
COPY config/ ./config/

# Start process
ENV PYTHONPATH="${PYTHONPATH}:/src"
ENTRYPOINT ["python", "emf/model_merger/worker.py"]
#ENTRYPOINT ["/bin/bash"]

FROM ${BASE_IMAGE} as emfos-task-generator

# Set working directory
WORKDIR /src

# Copy files to container
COPY emf/common/ ./emf/common/
COPY emf/task_generator/ ./emf/task_generator/
COPY config/ ./config/

# Start process
ENV PYTHONPATH="${PYTHONPATH}:/src"
ENTRYPOINT ["python", "emf/task_generator/worker.py"]
#ENTRYPOINT ["/bin/bash"]

FROM ${BASE_IMAGE} as emfos-model-quality

# Set working directory
WORKDIR /src

# Copy files to container
COPY emf/model_quality/ ./emf/model_quality/
COPY emf/common/ ./emf/common/
COPY config/ ./config/

# Start process
ENV PYTHONPATH="${PYTHONPATH}:/src"
ENTRYPOINT ["python", "emf/model_quality/worker.py"]
