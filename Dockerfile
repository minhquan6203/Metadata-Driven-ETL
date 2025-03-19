FROM python:3.9-slim

# Install Java for PySpark and other dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Set up Spark
ENV SPARK_VERSION=3.3.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Create Spark directory
RUN mkdir -p $SPARK_HOME

# Download and set up Spark
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/* ${SPARK_HOME}/ && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Set up pythonpath for PySpark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH

# Get Delta Lake jars
RUN wget -q https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.2.0/delta-core_2.12-2.2.0.jar -P $SPARK_HOME/jars/
RUN wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/2.2.0/delta-storage-2.2.0.jar -P $SPARK_HOME/jars/

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt /app/

# Install core Python packages
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir findspark

# Copy application code
COPY . .

# Make scripts executable
RUN chmod +x scripts/*.py

# Create data directories
RUN mkdir -p demo_data/raw demo_data/bronze demo_data/silver demo_data/gold demo_data/metadata

# Command to keep container running
CMD ["tail", "-f", "/dev/null"] 