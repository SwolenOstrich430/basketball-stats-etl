ARG AIRFLOW_BASE_IMAGE="apache/airflow:latest"
FROM ${AIRFLOW_BASE_IMAGE}

USER root
RUN apt-get update -y && apt-get install -y wget xvfb unzip jq

RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN apt-get install -y ./google-chrome-stable_current_amd64.deb

USER airflow 
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY . /tmp/