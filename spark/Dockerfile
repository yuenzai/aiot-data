FROM spark:3.5.5-scala2.12-java17-ubuntu

ARG SPARK_MAJOR_VERSION=3.5
ARG SPARK_VERSION=3.5.5
ARG ICEBERG_VERSION=1.8.1

RUN curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar

RUN curl -L -o /opt/spark/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar

RUN curl -L -o /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar
