# NOT WORKING ON ARM64!!
FROM python:3.11

RUN pip install --upgrade setuptools pip --user
RUN pip install pipenv --user

WORKDIR /code

# Install needed python modules
COPY Pipfile .
COPY Pipfile.lock .
RUN python -m pipenv install --system --deploy
# CMD [ "bash" ]


# Run from root folder
# sudo docker build -f ./docker/base/Dockerfile --tag emf-base .
# sudo docker run --rm -it --entrypoint bash emf-base