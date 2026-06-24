FROM jupyter/scipy-notebook:latest

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    rm -rf /var/lib/apt/lists/*
USER jovyan

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN pip install --no-cache-dir \
    pyiceberg[s3fs,datafusion,pyiceberg-core]==0.11.1 \
    pyarrow>=14.0.0 \
    pandas>=2.0.0 \
    matplotlib \
    jupyterlab \
    "fugue[sql]>=0.9.0" \
    fugue-jupyter \
    "duckdb>=1.0.0" \
    pyspark==3.5.6 \
    "dask[complete]>=2024.1.0" \
    dask-sql \
    "ray[default]>=2.9.0"

# Register %%fsql magic as a Jupyter startup script
RUN fugue-jupyter install startup
