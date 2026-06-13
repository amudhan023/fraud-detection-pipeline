package com.fraudpipeline.sinks

import com.fraudpipeline.utils._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.OffsetResetStrategy

import java.sql.{PreparedStatement, Timestamp}
import java.time.Duration

object SpendAnalyticsJob {

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

    val source = KafkaSource.builder[String]()
      .setBootstrapServers(cfg.kafka.getString("bootstrap-servers"))
      .setTopics(cfg.Topics.scored)
      .setGroupId(cfg.kafka.getString("consumer.group-id-spend"))
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
      .fromSource(source, watermarkStrategy, "kafka-scored-spend")
      .flatMap { json =>
        try List(JsonSerde.fromJson[ScoredTransaction](json))
        catch { case _: Exception => Nil }
      }

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

    val upsertSql =
      """INSERT INTO spend_aggregates
        |  (window_start, window_end, dimension, dimension_value, total_amount, txn_count)
        |VALUES (?, ?, ?, ?, ?, ?)
        |ON CONFLICT (window_start, window_end, dimension, dimension_value)
        |DO UPDATE SET
        |  total_amount = EXCLUDED.total_amount,
        |  txn_count    = EXCLUDED.txn_count
        |""".stripMargin

    val stmtBuilder: JdbcStatementBuilder[SpendAggregate] =
      (stmt: PreparedStatement, agg: SpendAggregate) => {
        stmt.setTimestamp(1, new Timestamp(agg.windowStart))
        stmt.setTimestamp(2, new Timestamp(agg.windowEnd))
        stmt.setString(3, agg.dimension)
        stmt.setString(4, agg.dimensionValue)
        stmt.setDouble(5, agg.totalAmount)
        stmt.setLong(6, agg.txnCount)
      }

    // Inline window function avoids the ProcessWindowFunction.Context type-projection issue in Scala
    def windowFn(dimension: String)(
      key   : String,
      window: TimeWindow,
      inputs: Iterable[(Double, Long)],
      out   : Collector[SpendAggregate]
    ): Unit = {
      val (totalAmount, txnCount) = inputs.head
      out.collect(SpendAggregate(window.getStart, window.getEnd, dimension, key, totalAmount, txnCount))
    }

    val agg = new SpendAggregateFunction

    val categoryAgg = scoredStream
      .keyBy(_.merchantCategory)
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .aggregate(agg, windowFn("CATEGORY") _)

    val merchantAgg = scoredStream
      .keyBy(_.merchantId)
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .aggregate(agg, windowFn("MERCHANT") _)

    val accountAgg = scoredStream
      .keyBy(_.accountId)
      .window(TumblingEventTimeWindows.of(Time.minutes(5)))
      .aggregate(agg, windowFn("ACCOUNT") _)

    categoryAgg.union(merchantAgg, accountAgg)
      .addSink(JdbcSink.sink(upsertSql, stmtBuilder, jdbcExecOptions, jdbcOptions))

    env.execute("FraudPipeline::SpendAnalytics")
  }
}

class SpendAggregateFunction
  extends AggregateFunction[ScoredTransaction, (Double, Long), (Double, Long)] {
  override def createAccumulator(): (Double, Long)                             = (0.0, 0L)
  override def add(v: ScoredTransaction, acc: (Double, Long)): (Double, Long) = (acc._1 + v.amount, acc._2 + 1)
  override def getResult(acc: (Double, Long)): (Double, Long)                 = acc
  override def merge(a: (Double, Long), b: (Double, Long)): (Double, Long)   = (a._1 + b._1, a._2 + b._2)
}
