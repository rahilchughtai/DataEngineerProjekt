# One image for Spark master, workers and the job containers (same Python and Spark version on
# driver and executors). The official Spark image ships Python and PySpark.
FROM docker.io/apache/spark:4.1.3-scala2.13-java17-python3-ubuntu

# S3A connector for accessing SeaweedFS, which the Spark image does not ship. hadoop-aws must
# match the Hadoop version in the image exactly (3.4.2); the AWS SDK bundle is the version
# hadoop-aws 3.4.2 is built against. The checksums secure the download.
ARG HADOOP_AWS_VERSION=3.4.2
ARG AWS_SDK_VERSION=2.29.52
ARG MAVEN=https://repo1.maven.org/maven2
USER root
RUN cd /opt/spark/jars \
 && wget -q "$MAVEN/org/apache/hadoop/hadoop-aws/$HADOOP_AWS_VERSION/hadoop-aws-$HADOOP_AWS_VERSION.jar" \
 && wget -q "$MAVEN/software/amazon/awssdk/bundle/$AWS_SDK_VERSION/bundle-$AWS_SDK_VERSION.jar" \
 && echo "d2d24e6b715b5656b9d238bf13b6682a07e2004dd36aad74a73aeb854319f4d9  hadoop-aws-$HADOOP_AWS_VERSION.jar" | sha256sum -c - \
 && echo "62638f33045690c6bc5e496b54b0b2e89a9ad38cbedacdf6e017fea6bc85f55c  bundle-$AWS_SDK_VERSION.jar" | sha256sum -c -

# Shared library (period, partitions) into the image's Python; build context is the repo root.
COPY libs/pipeline_common/pyproject.toml /tmp/pipeline_common/
COPY libs/pipeline_common/src /tmp/pipeline_common/src
RUN pip install --no-cache-dir --no-deps /tmp/pipeline_common && rm -rf /tmp/pipeline_common
COPY services/spark/spark-defaults.conf /opt/spark/conf/
COPY services/spark/jobs /opt/jobs

# The base image runs as the unprivileged user spark (uid 185).
USER spark
ENV PATH=/opt/spark/bin:$PATH PYTHONUNBUFFERED=1
