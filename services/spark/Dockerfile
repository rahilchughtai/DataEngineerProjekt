# Ein Image für Spark-Master, Worker und die Job-Container (gleiche Python- und Spark-Version
# auf Driver und Executors). Abweichung vom Service-Muster: Python und PySpark bringt das
# offizielle Spark-Image mit, die Jobs brauchen keine weiteren Pakete, daher kein uv sync.
FROM docker.io/apache/spark:4.1.3-scala2.13-java17-python3-ubuntu

# S3A-Connector für den Zugriff auf SeaweedFS. Spark liest Dateien über die Hadoop-FileSystem-API,
# liefert die S3-Implementierung aber nicht mit. Kein Hadoop-Cluster, nur diese zwei Jars.
# hadoop-aws muss exakt zur Hadoop-Version im Image passen (hadoop-client-api-3.4.2.jar),
# das AWS-SDK-Bundle ist die Version, gegen die hadoop-aws 3.4.2 gebaut ist.
ARG HADOOP_AWS_VERSION=3.4.2
ARG AWS_SDK_VERSION=2.29.52
ARG MAVEN=https://repo1.maven.org/maven2
USER root
RUN cd /opt/spark/jars \
 && wget -q "$MAVEN/org/apache/hadoop/hadoop-aws/$HADOOP_AWS_VERSION/hadoop-aws-$HADOOP_AWS_VERSION.jar" \
 && wget -q "$MAVEN/software/amazon/awssdk/bundle/$AWS_SDK_VERSION/bundle-$AWS_SDK_VERSION.jar" \
 && echo "d2d24e6b715b5656b9d238bf13b6682a07e2004dd36aad74a73aeb854319f4d9  hadoop-aws-$HADOOP_AWS_VERSION.jar" | sha256sum -c - \
 && echo "62638f33045690c6bc5e496b54b0b2e89a9ad38cbedacdf6e017fea6bc85f55c  bundle-$AWS_SDK_VERSION.jar" | sha256sum -c -

# Build-Kontext ist das Repo-Root, damit die gemeinsame Bibliothek ins Image kommt. Sie wird wie in
# Ingestion als Paket installiert, hier per pip ins Python des Images. spark_jobs bleibt Quellcode
# auf dem PYTHONPATH, weil spark-submit die Jobs als Dateipfad startet.
COPY libs/pipeline_common/pyproject.toml /tmp/pipeline_common/
COPY libs/pipeline_common/src /tmp/pipeline_common/src
RUN pip install --no-cache-dir --no-deps /tmp/pipeline_common && rm -rf /tmp/pipeline_common
COPY services/spark/conf/ /opt/spark/conf/
COPY --chmod=755 services/spark/bin/ /usr/local/bin/
COPY services/spark/src/spark_jobs /opt/spark-jobs/spark_jobs
# Ziel der Host-Kopie des Laufberichts; ein leeres benanntes Volume übernimmt den Besitzer.
RUN mkdir /runs && chown spark /runs

# Das Basisimage läuft bereits als unprivilegierter Nutzer spark (uid 185).
USER spark
WORKDIR /opt/spark-jobs
ENV PATH=/opt/spark/bin:$PATH PYTHONPATH=/opt/spark-jobs PYTHONUNBUFFERED=1
