# spark/Dockerfile
FROM ubuntu:22.04

# Install required packages
RUN apt-get update && apt-get install -y \
    openjdk-8-jdk \
    wget \
    curl \
    vim \
    maven \
    netcat \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop

COPY spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz /tmp/

# Download and install Spark
RUN tar -xzf /tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME && \
    rm /tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Install Hadoop for configuration
ENV HADOOP_VERSION_FULL=3.3.6
ENV HADOOP_HOME=/opt/hadoop


COPY hadoop-${HADOOP_VERSION_FULL}.tar.gz /tmp/

RUN tar -xzf /tmp/hadoop-${HADOOP_VERSION_FULL}.tar.gz && \
    mv hadoop-${HADOOP_VERSION_FULL} $HADOOP_HOME && \
    rm /tmp/hadoop-${HADOOP_VERSION_FULL}.tar.gz

# Create necessary directories
RUN mkdir -p /spark/logs /app /scripts

# Copy Spark configuration
COPY spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf

COPY config/* $HADOOP_CONF_DIR/

RUN echo "worker1" >> ${HADOOP_CONF_DIR}/workers
RUN echo "worker2" >> ${HADOOP_CONF_DIR}/workers

# Set JAVA_HOME in spark-env.sh
RUN echo "export JAVA_HOME=${JAVA_HOME}" > $SPARK_HOME/conf/spark-env.sh && \
    echo "export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}" >> $SPARK_HOME/conf/spark-env.sh && \
    echo "export YARN_CONF_DIR=${HADOOP_CONF_DIR}" >> $ $SPARK_HOME/conf/spark-env.sh

WORKDIR /app

# Expose ports
EXPOSE 8080 7077 4040 18080

CMD ["bash"]