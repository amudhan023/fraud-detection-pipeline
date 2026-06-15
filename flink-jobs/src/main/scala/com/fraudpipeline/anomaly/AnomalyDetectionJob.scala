package com.fraudpipeline.anomaly

import com.fraudpipeline.utils._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._
import org.apache.kafka.clients.consumer.OffsetResetStrategy

import java.sql.PreparedStatement
import java.time.Duration

class AnomalyStmtBuilder
    extends org.apache.flink.connector.jdbc.JdbcStatementBuilder[AnomalyEvent]
    with java.io.Serializable {
  override def accept(stmt: PreparedStatement, e: AnomalyEvent): Unit = {
    stmt.setString(1, e.transactionId)
    stmt.setString(2, e.accountId)
    stmt.setString(3, e.reasonCode)
    stmt.setString(4, JsonSerde.toJson(e.evidence))
    stmt.setLong(5, e.detectedAtMs)
  }
}

object AnomalyDetectionJob {

  def main(args: Array[String]): Unit = {
    val cfg = PipelineConfig
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(cfg.flink.getInt("parallelism"))

    env.enableCheckpointing(cfg.flink.getLong("checkpoint-interval-ms"))
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(cfg.flink.getLong("checkpoint-timeout-ms"))
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(cfg.flink.getLong("min-pause-between-ckpts"))
    env.getCheckpointConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L))

    val bootstrapServers = cfg.kafka.getString("bootstrap-servers")

    val source = KafkaSource.builder[String]()
      .setBootstrapServers(bootstrapServers)
      .setTopics(cfg.Topics.scored)
      .setGroupId(cfg.kafka.getString("consumer.group-id-anomaly"))
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
      .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
      .build()

    val watermarkStrategy = WatermarkStrategy
      .forBoundedOutOfOrderness[String](Duration.ofSeconds(5))
      .withIdleness(Duration.ofSeconds(10))
      .withTimestampAssigner(new SerializableTimestampAssigner[String] {
        override def extractTimestamp(element: String, recordTimestamp: Long): Long =
          try JsonSerde.fromJson[ScoredTransaction](element).eventTimeMs
          catch { case _: Exception => System.currentTimeMillis() }
      })

    val scoredStream: DataStream[ScoredTransaction] = env
      .fromSource(source, watermarkStrategy, "kafka-scored-transactions")
      .flatMap { json =>
        try List(JsonSerde.fromJson[ScoredTransaction](json))
        catch { case _: Exception => Nil }
      }

    val anomalyStream: DataStream[AnomalyEvent] = scoredStream
      .keyBy(_.accountId)
      .process(new CombinedAnomalyFunction(cfg.root))  // pass Config, not PipelineConfig.type

    anomalyStream.sinkTo(
      KafkaSinks.jsonSink[AnomalyEvent](bootstrapServers, cfg.Topics.anomalies, _.accountId)
    )

    val jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
      .withUrl(cfg.postgres.getString("url"))
      .withDriverName("org.postgresql.Driver")
      .withUsername(cfg.postgres.getString("user"))
      .withPassword(cfg.postgres.getString("password"))
      .build()
    val jdbcExecOptions = JdbcExecutionOptions.builder()
      .withBatchSize(100)
      .withBatchIntervalMs(2000L)
      .withMaxRetries(3)
      .build()

    anomalyStream.addSink(JdbcSink.sink(
      """INSERT INTO anomaly_events (transaction_id, account_id, reason_code, evidence, detected_at)
        |VALUES (?::uuid, ?::uuid, ?, ?::jsonb, to_timestamp(?/1000.0))
        |""".stripMargin,
      new AnomalyStmtBuilder(),
      jdbcExecOptions,
      jdbcOptions
    ))

    env.execute("FraudPipeline::AnomalyDetection")
  }
}
