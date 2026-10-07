FROM python:3.12-slim-bookworm

ARG PYSPARK_VERSION=4.1.3
ARG HADOOP_VERSION=3.4.2
ARG AWS_SDK_VERSION=2.29.52
ARG PG_JDBC_VERSION=42.7.13

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl tini \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java

RUN pip install --no-cache-dir \
      "pyspark==${PYSPARK_VERSION}" \
      "jupyterlab>=4.2,<5" ipykernel \
      pandas pyarrow matplotlib boto3 "psycopg[binary]"

ENV JAVA_HOME=/opt/java \
    SPARK_HOME=/usr/local/lib/python3.12/site-packages/pyspark \
    PYSPARK_PYTHON=python3
ENV PATH="${PATH}:${SPARK_HOME}/bin"

# Коннекторы кладём прямо в jars PySpark: PostgreSQL JDBC, S3A и spark-hadoop-cloud (magic committer)
RUN cd "${SPARK_HOME}/jars" \
 && M=https://repo1.maven.org/maven2 \
 && curl -fsSLO "${M}/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar" \
 && curl -fsSLO "${M}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
 && curl -fsSLO "${M}/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar" \
 && curl -fsSLO "${M}/org/apache/spark/spark-hadoop-cloud_2.13/${PYSPARK_VERSION}/spark-hadoop-cloud_2.13-${PYSPARK_VERSION}.jar"

RUN useradd -m -u 1000 -s /bin/bash jovyan \
 && mkdir -p /home/jovyan/work /home/jovyan/spark-events \
 && chown -R jovyan:jovyan /home/jovyan

USER jovyan
WORKDIR /home/jovyan/work
ENTRYPOINT ["tini", "--"]
