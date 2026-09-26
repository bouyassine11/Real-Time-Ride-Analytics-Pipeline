# ============================================================
# Custom Spark image for the ride analytics pipeline
# Base: official Apache Spark 3.5.1 with Python 3
# Adds: PostgreSQL JDBC driver + Kafka connector JARs
# ============================================================

FROM apache/spark:3.5.1-python3

USER root

# Install Python packages
RUN pip install --no-cache-dir \
    python-dotenv==1.0.1 \
    psycopg2-binary==2.9.9

# Install wget to download JARs
RUN apt-get update && apt-get install -y --no-install-recommends wget \
    && rm -rf /var/lib/apt/lists/*

# Kafka connector for Spark 3.5 / Scala 2.12
RUN wget -q -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar

# Kafka clients (dependency of spark-sql-kafka)
RUN wget -q -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar

# Spark token provider for Kafka
RUN wget -q -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar

# Commons pool2 (dependency of kafka-clients)
RUN wget -q -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# PostgreSQL JDBC driver — used by PySpark to write KPIs to PostgreSQL
RUN wget -q -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

# Keep running as root so Spark can write checkpoints to /tmp
