# Define base container image
FROM python:3.11 as base

ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONFAULTHANDLER 1

FROM base as deps

RUN pip install --upgrade setuptools pip --user
RUN pip install pipenv --user

# Install pipenv
COPY Pipfile .
COPY Pipfile.lock .
RUN PIPENV_VENV_IN_PROJECT=1 pipenv install --deploy

# Make runtime container to have smaller size
FROM base as runtime
COPY --from=deps /.venv /.venv
ENV PATH="/.venv/bin:$PATH"

# Set working directory
WORKDIR /code

# Copy file to container
COPY emf/model_retriever/ ./emf/model_retriever/
COPY emf/loadflow_tool/ ./emf/loadflow_tool/
COPY emf/common/ ./emf/common/
COPY config/ ./config/

EXPOSE 80

ENTRYPOINT ["python", "emf/model_retriever/worker.py"]
# Run from root folder
# sudo docker build -f ./docker/base/Dockerfile --tag emf-base .
# sudo docker run --rm -it --entrypoint bash emf-base