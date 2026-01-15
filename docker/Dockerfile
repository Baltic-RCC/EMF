# Define base container image (platform has to be defined if using with ARM64 architecture)
FROM python:3.11-slim as emfos-base
#FROM --platform=linux/amd64 python:3.11-slim as emfos-base

# Set working directory
WORKDIR /src

# Install tools to set up python environment
RUN apt-get update && apt-get install -y --no-install-recommends procps && rm -rf /var/lib/apt/lists/*
RUN pip install --upgrade setuptools pip uv pipenv

# Install needed modules
COPY Pipfile .
RUN pipenv lock
#COPY Pipfile.lock .
#RUN PIPENV_VENV_IN_PROJECT=1 pipenv install --deploy
#RUN pipenv install --system --deploy
RUN pipenv requirements > requirements.txt
RUN uv pip sync requirements.txt --system
#RUN uv cache clean

# Set to use interpreter from venv (if venv is used)
#ENV PATH="/.venv/bin:$PATH"

FROM balticrcc/emfos-base:pypow1.13 as emfos-model-retriever

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

FROM balticrcc/emfos-base:pypow1.13 as emfos-local-model-retriever

# Set working directory
WORKDIR /src

# Copy files to container
COPY emf/model_retriever/ ./emf/model_retriever/
COPY emf/common/ ./emf/common/
COPY config/ ./config/

# Start process
ENV PYTHONPATH="${PYTHONPATH}:/src"
ENTRYPOINT ["python", "emf/model_retriever/local_worker.py"]

FROM balticrcc/emfos-base:pypow1.13 as emfos-model-validator

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

FROM balticrcc/emfos-base:pypow1.13 as emfos-model-merger

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

FROM balticrcc/emfos-base:pypow1.13 as emfos-task-generator

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

# Model Quality image
FROM balticrcc/emfos-base:pypow1.13 as emfos-model-quality

WORKDIR /src

COPY emf/model_quality/ ./emf/model_quality/
COPY emf/common/ ./emf/common/
COPY config/ ./config/

ENV PYTHONPATH="${PYTHONPATH}:/src"
ENTRYPOINT ["python", "emf/model_quality/worker.py"]
