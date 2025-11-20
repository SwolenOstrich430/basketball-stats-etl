ARG AIRFLOW_BASE_IMAGE="apache/airflow:latest"
ARG CHROME_WEB_DRIVER_DOWNLOAD_URL="https://rpmfind.net/linux/fedora/linux/development/rawhide/Everything/aarch64/os/Packages/c/chromedriver-142.0.7444.162-2.fc44.aarch64.rpm"
ARG CHROME_WEB_DRIVER_FILE_NAME="chrome_web_diver.rpm"
FROM ${AIRFLOW_BASE_IMAGE}

USER root

RUN apt-get update && apt-get install -y \
    wget \
    gnupg2 \
    --no-install-recommends && \
    wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && apt-get install -y google-chrome-stable && \
    rm -rf /var/lib/apt/lists/*

USER airflow 
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY . /tmp/