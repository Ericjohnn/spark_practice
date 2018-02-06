FROM gettyimages/spark:2.2.0-hadoop-2.7

ENV SPARK_HOME=/usr/local/spark

## s3 support
ENV SPARK_VERSION 2.2.0
ENV SPARK_PACKAGE spark-${SPARK_VERSION}-bin-without-hadoop
ENV SPARK_HOME /usr/spark-${SPARK_VERSION}

RUN mkdir -p ${SPARK_HOME}/jars/ && cd ${SPARK_HOME}/jars/ &&\
    curl -O http://central.maven.org/maven2/org/apache/kafka/kafka-clients/1.0.0/kafka-clients-1.0.0.jar &&\
    curl -O http://central.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.11/2.2.0/spark-sql-kafka-0-10_2.11-2.2.0.jar &&\
    curl -O http://central.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-8-assembly_2.11/2.2.0/spark-streaming-kafka-0-8-assembly_2.11-2.2.0.jar &&\
    curl -O http://central.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-8_2.11/2.2.0/spark-streaming-kafka-0-8_2.11-2.2.0.jar

COPY conf/  ${SPARK_HOME}/conf
