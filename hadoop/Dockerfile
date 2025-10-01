# hadoop/Dockerfile
FROM ubuntu:22.04

# Install required packages
RUN apt-get update && apt-get install -y \
    openjdk-8-jdk \
    wget \
    curl \
    ssh \
    rsync \
    vim \
    net-tools \
    netcat \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Download and install Hadoop
COPY hadoop-${HADOOP_VERSION}.tar.gz /tmp/

# Download and install Hadoop
RUN tar -xzf /tmp/hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} $HADOOP_HOME && \
    rm /tmp/hadoop-${HADOOP_VERSION}.tar.gz

# Create directories
RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data $HADOOP_HOME/logs /scripts

# Add configuration files
COPY config/* $HADOOP_CONF_DIR/

# Copy startup script
COPY scripts/start-hdfs.sh /scripts/start-hdfs.sh

# Set JAVA_HOME in hadoop-env.sh
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> $HADOOP_CONF_DIR/hadoop-env.sh

# Make scripts executable
RUN chmod +x /scripts/*.sh

WORKDIR $HADOOP_HOME

# Expose ports
EXPOSE 9870 8020 9864 8088 8042 8030 8031 8032

CMD ["bash"]