# Define base container image (platform has to be defined if using with ARM64 architecture)
FROM python:3.11 as emfos-model-retriever
#FROM --platform=linux/amd64 python:3.11

# Install tools to set up python environment
RUN pip install --upgrade setuptools
RUN pip install --upgrade pip
RUN pip install uv
#RUN pip install pipenv

# Install needed modules
COPY Pipfile .
COPY Pipfile.lock .
COPY requirements.txt .
#RUN PIPENV_VENV_IN_PROJECT=1 pipenv install --deploy
#RUN pipenv install --system --deploy
#RUN pipenv requirements > requirements.txt
RUN uv pip sync requirements.txt --system
#RUN uv cache clean

# Set to use interpreter from venv (if venv is used)
#ENV PATH="/.venv/bin:$PATH"

# Set working directory
WORKDIR /src

# Copy files to container
COPY emf/model_retriever/ ./emf/model_retriever/
COPY emf/loadflow_tool/ ./emf/loadflow_tool/
COPY emf/common/ ./emf/common/
COPY config/ ./config/

# Start process
ENV PYTHONPATH="${PYTHONPATH}:/src"
ENTRYPOINT ["python", "emf/model_retriever/worker.py"]
#ENTRYPOINT ["/bin/bash"]

FROM python:3.11 as emfos-model-merger
#FROM --platform=linux/amd64 python:3.11

# Install tools to set up python environment
RUN pip install --upgrade setuptools
RUN pip install --upgrade pip
RUN pip install uv
#RUN pip install pipenv

# Install needed modules
COPY Pipfile .
COPY Pipfile.lock .
COPY requirements.txt .
#RUN PIPENV_VENV_IN_PROJECT=1 pipenv install --deploy
#RUN pipenv install --system --deploy
#RUN pipenv requirements > requirements.txt
RUN uv pip sync requirements.txt --system
#RUN uv cache clean

# Set to use interpreter from venv (if venv is used)
#ENV PATH="/.venv/bin:$PATH"

# Set working directory
WORKDIR /src

# Copy files to container
COPY emf/loadflow_tool/ ./emf/loadflow_tool/
COPY emf/common/ ./emf/common/
COPY emf/task_generator/ ./emf/task_generator/
COPY config/ ./config/

# Start process
ENV PYTHONPATH="${PYTHONPATH}:/src"
ENTRYPOINT ["python", "emf/loadflow_tool/model_merger/worker.py"]
#ENTRYPOINT ["/bin/bash"]