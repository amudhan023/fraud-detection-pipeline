val flinkVersion     = "1.18.1"
val scalaVersion_    = "2.12.17"
val kafkaConnVersion = "3.1.0-1.18"
val jdbcConnVersion  = "3.1.2-1.18"
val avroVersion      = "1.11.3"
val otelVersion      = "1.32.0"

ThisBuild / scalaVersion  := scalaVersion_
ThisBuild / organization  := "com.fraudpipeline"
ThisBuild / version       := "0.1.0-SNAPSHOT"

lazy val root = (project in file("."))
  .settings(
    name := "fraud-detection-flink-jobs",

    resolvers ++= Seq(
      "Confluent" at "https://packages.confluent.io/maven/"
    ),

    libraryDependencies ++= Seq(
      // ── Flink runtime (provided by cluster) ─────────────────────────────
      "org.apache.flink" %% "flink-scala"                 % flinkVersion % "provided",
      "org.apache.flink" %% "flink-streaming-scala"       % flinkVersion % "provided",
      "org.apache.flink"  % "flink-runtime"               % flinkVersion % "provided",
      "org.apache.flink"  % "flink-clients"               % flinkVersion % "provided",
      "org.apache.flink"  % "flink-statebackend-rocksdb"  % flinkVersion % "provided",
      "org.apache.flink"  % "flink-metrics-prometheus"    % flinkVersion % "provided",
      "org.apache.flink"  % "flink-connector-base"        % flinkVersion % "provided",

      // ── Kafka connector (externalized in Flink 1.17+) ───────────────────
      "org.apache.flink"  % "flink-connector-kafka"       % kafkaConnVersion,

      // ── JDBC connector ──────────────────────────────────────────────────
      "org.apache.flink"  % "flink-connector-jdbc"        % jdbcConnVersion,
      "org.postgresql"    % "postgresql"                  % "42.7.1",

      // ── JSON serialization ───────────────────────────────────────────────
      "com.fasterxml.jackson.core"   % "jackson-databind"         % "2.15.3",
      "com.fasterxml.jackson.module" %% "jackson-module-scala"    % "2.15.3",
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.15.3",

      // ── OpenTelemetry ────────────────────────────────────────────────────
      "io.opentelemetry" % "opentelemetry-api"            % otelVersion,
      "io.opentelemetry" % "opentelemetry-sdk"            % otelVersion,
      "io.opentelemetry" % "opentelemetry-exporter-otlp"  % otelVersion,
      "io.opentelemetry" % "opentelemetry-sdk-extension-autoconfigure" % otelVersion,

      // ── Config ───────────────────────────────────────────────────────────
      "com.typesafe"     % "config"                       % "1.4.3",

      // ── Logging ──────────────────────────────────────────────────────────
      "org.apache.logging.log4j" % "log4j-slf4j-impl"    % "2.22.1",
      "org.apache.logging.log4j" % "log4j-api"           % "2.22.1",
      "org.apache.logging.log4j" % "log4j-core"          % "2.22.1",

      // ── Test ─────────────────────────────────────────────────────────────
      "org.apache.flink"  % "flink-test-utils"            % flinkVersion % Test,
      "org.scalatest"    %% "scalatest"                    % "3.2.17"    % Test
    ),

    assembly / assemblyJarName := s"fraud-pipeline-${version.value}.jar",

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*)            => MergeStrategy.concat
      case PathList("META-INF", "versions", _, "module-info.class") => MergeStrategy.discard
      case PathList("META-INF", _*)                        => MergeStrategy.discard
      case PathList("module-info.class")                   => MergeStrategy.discard
      case "reference.conf"                                => MergeStrategy.concat
      case "log4j2.xml"                                    => MergeStrategy.first
      case _                                               => MergeStrategy.first
    }
  )
