# @Author: Tanel Treuberg
# @Github: https://github.com/The-Magicians-Code
# @Description: Create the development Docker container

FROM python:3.11
WORKDIR /code

# Copy installation files
COPY Pipfile .
COPY Pipfile.lock .
# Install dependencies for pypowsybl
RUN apt-get update \
&& apt-get install -y build-essential cmake gcc clang clang-tools wget libz-dev zlib1g-dev \
&& apt-get install -y default-jre-headless && apt-get clean && rm -rf /var/lib/apt/lists/* \
&& apt-get update && apt-get install -y maven
# RUN wget https://github.com/graalvm/graalvm-ce-builds/releases/download/jdk-20.0.1/graalvm-community-jdk-20.0.1_linux-x64_bin.tar.gz

# Crashes here upon trying to install pypowsybl ...
# RUN pip install pypowsybl

# This is a temporary solution for all other packages
COPY requirements.txt .
RUN pip install -r requirements.txt

# Install packages to the system without creating virtual environment
# RUN pip install pipenv
# RUN python3-m pipenv install --system --deploy

CMD ["bash"]