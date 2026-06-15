FROM flink:1.18.1-scala_2.12-java11

# Download required JARs from Maven Central at build time.
# (Binary JARs are not stored in git; this keeps the repo lightweight.)
RUN curl -fsSL -o /opt/flink/lib/flink-metrics-prometheus-1.18.1.jar \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/1.18.1/flink-metrics-prometheus-1.18.1.jar" && \
    curl -fsSL -o /opt/flink/lib/flink-statebackend-rocksdb-1.18.1.jar \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-statebackend-rocksdb/1.18.1/flink-statebackend-rocksdb-1.18.1.jar"

# GCS filesystem plugin — enables gs:// checkpoint paths required for GCP/GKE deployment.
# The official Flink image ships this jar in /opt/flink/opt/; if absent, download from Maven.
RUN mkdir -p /opt/flink/plugins/gs-fs-hadoop && \
    (cp /opt/flink/opt/flink-gs-fs-hadoop-1.18.1.jar /opt/flink/plugins/gs-fs-hadoop/ 2>/dev/null || \
     curl -fsSL -o /opt/flink/plugins/gs-fs-hadoop/flink-gs-fs-hadoop-1.18.1.jar \
       "https://repo1.maven.org/maven2/org/apache/flink/flink-gs-fs-hadoop/1.18.1/flink-gs-fs-hadoop-1.18.1.jar")

USER flink
