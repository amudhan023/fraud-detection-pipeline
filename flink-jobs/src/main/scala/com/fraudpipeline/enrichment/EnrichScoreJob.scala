package com.fraudpipeline.enrichment

import com.fraudpipeline.utils._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._
import org.apache.kafka.clients.consumer.OffsetResetStrategy

import java.sql.PreparedStatement
import java.time.Duration

class ScoredTxnStmtBuilder
    extends org.apache.flink.connector.jdbc.JdbcStatementBuilder[ScoredTransaction]
    with java.io.Serializable {
  override def accept(stmt: PreparedStatement, t: ScoredTransaction): Unit = {
    stmt.setString(1, t.transactionId)
    stmt.setString(2, t.accountId)
    stmt.setString(3, t.merchantId)
    stmt.setDouble(4, t.amount)
    stmt.setDouble(5, t.riskScore)
    stmt.setString(6, JsonSerde.toJson(t.scoreBreakdown))
    stmt.setLong(7, t.eventTimeMs)
  }
}

object EnrichScoreJob {

  val merchantStateDesc: MapStateDescriptor[String, MerchantProfile] =
    new MapStateDescriptor[String, MerchantProfile]("merchantBroadcast", classOf[String], classOf[MerchantProfile])

  val accountStateDesc: MapStateDescriptor[String, AccountProfile] =
    new MapStateDescriptor[String, AccountProfile]("accountBroadcast", classOf[String], classOf[AccountProfile])

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
    val strSchema        = new org.apache.flink.api.common.serialization.SimpleStringSchema()

    val rawSource = KafkaSource.builder[String]()
      .setBootstrapServers(bootstrapServers)
      .setTopics(cfg.Topics.raw)
      .setGroupId(cfg.kafka.getString("consumer.group-id-enrich-score"))
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
      .setValueOnlyDeserializer(strSchema)
      .build()

    val merchantSource = KafkaSource.builder[String]()
      .setBootstrapServers(bootstrapServers)
      .setTopics(cfg.Topics.merchants)
      .setGroupId("flink-merchant-broadcast-cg")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(strSchema)
      .build()

    val accountSource = KafkaSource.builder[String]()
      .setBootstrapServers(bootstrapServers)
      .setTopics(cfg.Topics.accounts)
      .setGroupId("flink-account-broadcast-cg")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(strSchema)
      .build()

    val watermarkStrategy = WatermarkStrategy
      .forBoundedOutOfOrderness[String](Duration.ofSeconds(5))
      .withIdleness(Duration.ofSeconds(cfg.flink.getLong("watermark-idle-timeout-ms") / 1000))
      .withTimestampAssigner(new SerializableTimestampAssigner[String] {
        override def extractTimestamp(element: String, recordTimestamp: Long): Long =
          JsonSerde.parseDebeziumTransaction(element)
            .map(_.eventTimeMs)
            .getOrElse(System.currentTimeMillis())
      })

    val rawStream = env.fromSource(rawSource, watermarkStrategy, "kafka-raw-transactions")

    val merchantStream = env.fromSource(
      merchantSource, WatermarkStrategy.noWatermarks[String](), "kafka-merchant-lookup"
    )
    val accountStream = env.fromSource(
      accountSource, WatermarkStrategy.noWatermarks[String](), "kafka-account-lookup"
    )

    // Parse Debezium envelope → RawTransaction
    val parsedStream: DataStream[RawTransaction] = rawStream
      .flatMap { json => JsonSerde.parseDebeziumTransaction(json).filter(_.op != "d").toList }

    // Enrich with merchant broadcast state (BroadcastProcessFunction requires non-keyed stream)
    val withMerchant: DataStream[(RawTransaction, Option[MerchantProfile])] =
      parsedStream
        .connect(merchantStream.broadcast(merchantStateDesc))
        .process(new MerchantEnrichmentFunction())

    // Enrich with account broadcast state (BroadcastProcessFunction requires non-keyed stream)
    val fullyEnriched: DataStream[(RawTransaction, Option[MerchantProfile], Option[AccountProfile])] =
      withMerchant
        .connect(accountStream.broadcast(accountStateDesc))
        .process(new AccountEnrichmentFunction())

    // Score
    val scoredStream: DataStream[ScoredTransaction] =
      fullyEnriched
        .keyBy(_._1.accountId)
        .process(new com.fraudpipeline.scoring.RiskScoringFunction())

    scoredStream.sinkTo(
      KafkaSinks.jsonSink[ScoredTransaction](bootstrapServers, cfg.Topics.scored, _.accountId)
    )

    val jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
      .withUrl(cfg.postgres.getString("url"))
      .withDriverName("org.postgresql.Driver")
      .withUsername(cfg.postgres.getString("user"))
      .withPassword(cfg.postgres.getString("password"))
      .build()
    val jdbcExecOptions = JdbcExecutionOptions.builder()
      .withBatchSize(200)
      .withBatchIntervalMs(2000L)
      .withMaxRetries(3)
      .build()

    scoredStream.addSink(JdbcSink.sink(
      """INSERT INTO scored_transactions
        |  (transaction_id, account_id, merchant_id, amount, risk_score, score_breakdown, event_time)
        |VALUES (?::uuid, ?::uuid, ?::uuid, ?, ?, ?::jsonb, to_timestamp(?/1000.0))
        |ON CONFLICT (transaction_id) DO UPDATE SET
        |  risk_score = EXCLUDED.risk_score,
        |  score_breakdown = EXCLUDED.score_breakdown,
        |  processed_at = NOW()
        |""".stripMargin,
      new ScoredTxnStmtBuilder(),
      jdbcExecOptions,
      jdbcOptions
    ))

    env.execute("FraudPipeline::EnrichScore")
  }
}
