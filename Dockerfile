FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir -p /tmp/spark-events && chmod 777 /tmp/spark-events

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
RUN pip install --no-cache-dir \
    requests \
    apache-airflow-providers-apache-spark==4.11.0 \
    pyspark==3.5.6
