FROM flink:1.18.1-scala_2.12-java11

# Copy pre-cached JARs (avoids network download during build)
COPY flink-metrics-prometheus-1.18.1.jar /opt/flink/lib/
COPY flink-statebackend-rocksdb-1.18.1.jar /opt/flink/lib/

USER flink
