FROM ubuntu:latest

RUN apt-get update && apt-get install -y \
    openjdk-11-jdk curl wget tar iputils-ping

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
ENV SPARK_VERSION=3.5.3
ENV HADOOP_VERSION=3

RUN wget https://dlcdn.apache.org/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz \
    && tar -xzf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz -C /opt \
    && rm spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz \
    && ln -s /opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

COPY dependencies/hadoop-aws-3.3.4.jar "$SPARK_HOME/jars"
COPY dependencies/aws-java-sdk-bundle-1.12.771.jar "$SPARK_HOME/jars"

ENV MINIO_VERSION=latest
RUN wget https://dl.min.io/server/minio/release/linux-amd64/minio \
    && chmod +x minio \
    && mv minio /usr/local/bin/

RUN wget https://dl.min.io/client/mc/release/linux-amd64/mc \
    && chmod +x mc \
    && mv mc /usr/local/bin/

ENV MINIO_ROOT_USER=${MINIO_ROOT_USER}
ENV MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD}
ENV MINIO_DATA_DIR=/data

COPY spark-minio-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/spark-minio-entrypoint.sh

CMD ["spark-minio-entrypoint.sh"]
