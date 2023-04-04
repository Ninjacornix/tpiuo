# install kafka and zookeeper
# install dependencies
# copy python scripts to container

FROM openjdk:11-jre-slim

# install kafka and zookeeper

RUN apt-get update && apt-get install -y wget && \
    wget https://downloads.apache.org/kafka/3.4.0/kafka_2.12-3.4.0.tgz && \
    tar -xzf kafka_2.12-3.4.0.tgz && \
    rm kafka_2.12-3.4.0.tgz && \
    mv kafka_2.12-3.4.0 kafka

RUN apt-get update && apt-get install -y wget && \
    wget https://downloads.apache.org/zookeeper/zookeeper-3.8.1/apache-zookeeper-3.8.1-bin.tar.gz && \
    tar -xzf apache-zookeeper-3.8.1-bin.tar.gz && \
    rm apache-zookeeper-3.8.1-bin.tar.gz && \
    mv apache-zookeeper-3.8.1-bin zookeeper

WORKDIR /kafka

# install dependencies

RUN apt-get update && apt-get install -y python3 python3-pip && \
    pip3 install kafka-python

# copy python scripts to container

COPY ./producer.py /kafka
COPY ./consumer.py /kafka

# run scripts
CMD ["python3", "producer.py"] && ["python3", "consumer.py"]

EXPOSE 9092