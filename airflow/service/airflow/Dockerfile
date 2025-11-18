ARG AIRFLOW_BASE_IMAGE="apache/airflow:2.0.0-python3.8"
FROM ${AIRFLOW_BASE_IMAGE}

RUN pip install --user --no-cache-dir \
    pyarrow==0.17.1 \
    minio==5.0.10 \
    geopandas==0.8.0 \
    pygeos==0.7.1 \
    apache-airflow-providers-amazon~=1.0.0