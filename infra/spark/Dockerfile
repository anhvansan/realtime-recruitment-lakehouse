# ==============================================================================
# CPC AdTech Lakehouse - Apache Spark 3.5.3 + Delta Lake 3.2.1 + S3A + DuckDB
# Official Apache Spark image (NO latest, NO bitnami/*) — Rule R4
# ==============================================================================
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

# Pin exact versions for reproducible builds (matches docs/versions.md)
ARG SPARK_VERSION=3.5.3
ARG SCALA_BINARY_VERSION=2.12
ARG DELTA_VERSION=3.2.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262

ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    SPARK_HOME=/opt/spark \
    HOME=/home/spark \
    IVY_CACHE_DIR=/home/spark/.ivy2 \
    DUCKDB_EXTENSION_DIRECTORY=/opt/duckdb_extensions

# 1. Install pinned Python dependencies (Delta, DuckDB, Config, S3 client)
RUN pip3 install --no-cache-dir \
    delta-spark==${DELTA_VERSION} \
    duckdb==1.5.6 \
    pydantic==2.9.2 \
    pydantic-settings==2.6.1 \
    pyyaml==6.0.2 \
    python-dotenv==1.0.1 \
    boto3==1.35.99

# 2. Bake exact pinned JARs into /opt/spark/jars (IPv4-only, retry + Content-Length check, zero Ivy POM overhead)
COPY infra/spark/download_jars.py /tmp/download_jars.py
RUN python3 /tmp/download_jars.py --group aws --dest /opt/spark/jars
RUN python3 /tmp/download_jars.py --group core --dest /opt/spark/jars && \
    rm -f /tmp/download_jars.py

# 3. Pre-install DuckDB httpfs & delta extensions into shared directory
RUN mkdir -p /opt/duckdb_extensions /home/spark/.ivy2 /app && \
    python3 -c "import duckdb; con = duckdb.connect(':memory:', config={'extension_directory': '/opt/duckdb_extensions'}); con.execute('INSTALL httpfs'); con.execute('INSTALL delta'); print('DuckDB extensions baked successfully')" && \
    chown -R spark:spark /home/spark /opt/duckdb_extensions /app

WORKDIR /app
USER spark

CMD ["tail", "-f", "/dev/null"]
