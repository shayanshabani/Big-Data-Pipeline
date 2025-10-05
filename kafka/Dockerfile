FROM ubuntu:22.04

# Install required packages
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    wget \
    curl \
    netcat \
    vim \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV KAFKA_VERSION=3.7.0
ENV SCALA_VERSION=2.13
ENV KAFKA_HOME=/opt/kafka
ENV PATH=$PATH:$KAFKA_HOME/bin

COPY kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz /tmp/


# Download and install Kafka
RUN tar -xzf /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C /opt && \
    mv /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} ${KAFKA_HOME} && \
    rm /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz

# Create directories
RUN mkdir -p /var/lib/kafka/data /var/log/kafka /opt/kafka/config/kraft

# Copy startup script
COPY start-kafka.sh /usr/local/bin/start-kafka.sh
RUN chmod +x /usr/local/bin/start-kafka.sh

WORKDIR ${KAFKA_HOME}

# Expose ports
# 9092-9096: External client connections
# 9093-9097: Controller ports
# 19092: Internal broker communication
EXPOSE 9092 9093 9094 9095 9096 9097 19092

CMD ["/usr/local/bin/start-kafka.sh"]
