package com.fraudpipeline.anomaly

import com.fraudpipeline.utils._
import com.typesafe.config.Config
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.math._

// Type aliases so we don't repeat the full parameterized type in every method signature
private object CAFTypes {
  type KPF = KeyedProcessFunction[String, ScoredTransaction, AnomalyEvent]
  type Ctx        = KPF#Context
  type TimerCtx   = KPF#OnTimerContext
}

class CombinedAnomalyFunction(cfg: Config)
  extends KeyedProcessFunction[String, ScoredTransaction, AnomalyEvent] {

  import CAFTypes._

  private val log = LoggerFactory.getLogger(getClass)

  @transient private var velocityMax    : Int    = _
  @transient private var velocityWindow : Long   = _
  @transient private var zScoreThreshold: Double = _
  @transient private var zScoreMinHist  : Int    = _
  @transient private var maxSpeedKmh    : Double = _

  @transient private var velocityState     : ListState[Long]                   = _
  @transient private var statsState        : ValueState[RunningStats]          = _
  @transient private var lastLocationState : ValueState[(Double, Double, Long)] = _

  override def open(parameters: Configuration): Unit = {
    velocityMax     = cfg.getConfig("pipeline.anomaly.velocity").getInt("max-transactions")
    velocityWindow  = cfg.getConfig("pipeline.anomaly.velocity").getLong("window-seconds") * 1000L
    zScoreThreshold = cfg.getConfig("pipeline.anomaly.amount-spike").getDouble("z-score-threshold")
    zScoreMinHist   = cfg.getConfig("pipeline.anomaly.amount-spike").getInt("min-history-count")
    maxSpeedKmh     = cfg.getConfig("pipeline.anomaly.geo-travel").getDouble("max-speed-kmh")

    velocityState = getRuntimeContext.getListState(
      new ListStateDescriptor[Long]("velocity-timestamps", classOf[Long])
    )
    statsState = getRuntimeContext.getState(
      new ValueStateDescriptor[RunningStats]("amount-stats", classOf[RunningStats])
    )
    lastLocationState = getRuntimeContext.getState(
      new ValueStateDescriptor[(Double, Double, Long)](
        "last-location", classOf[(Double, Double, Long)]
      )
    )
  }

  override def processElement(txn: ScoredTransaction, ctx: Ctx, out: Collector[AnomalyEvent]): Unit = {
    checkVelocity(txn, ctx, out)
    checkAmountSpike(txn, out)
    checkGeoTravel(txn, out)
  }

  override def onTimer(timestamp: Long, ctx: TimerCtx, out: Collector[AnomalyEvent]): Unit = {
    val cutoff    = timestamp - velocityWindow - 1000L
    val remaining = velocityState.get().asScala.filter(_ >= cutoff).toList
    velocityState.update(remaining.asJava)
  }

  private def checkVelocity(txn: ScoredTransaction, ctx: Ctx, out: Collector[AnomalyEvent]): Unit = {
    val now         = txn.eventTimeMs
    val windowStart = now - velocityWindow

    velocityState.add(now)
    val fresh = velocityState.get().asScala.filter(_ >= windowStart).toList
    velocityState.update(fresh.asJava)

    if (fresh.size > velocityMax) {
      log.debug(s"VELOCITY flag: account=${txn.accountId} count=${fresh.size} window=${velocityWindow}ms")
      out.collect(AnomalyEvent(
        transactionId = txn.transactionId,
        accountId     = txn.accountId,
        reasonCode    = "VELOCITY",
        evidence      = Map(
          "txn_count" -> fresh.size.toString,
          "window_ms" -> velocityWindow.toString,
          "threshold" -> velocityMax.toString
        ),
        detectedAtMs  = System.currentTimeMillis()
      ))
      ctx.timerService().registerEventTimeTimer(now + velocityWindow + 1000L)
    }
  }

  private def checkAmountSpike(txn: ScoredTransaction, out: Collector[AnomalyEvent]): Unit = {
    val stats = Option(statsState.value()).getOrElse(RunningStats.empty)

    if (stats.count >= zScoreMinHist) {
      val z = stats.zScore(txn.amount)
      if (z > zScoreThreshold) {
        log.debug(s"AMOUNT_SPIKE flag: account=${txn.accountId} z=$z amount=${txn.amount}")
        out.collect(AnomalyEvent(
          transactionId = txn.transactionId,
          accountId     = txn.accountId,
          reasonCode    = "AMOUNT_SPIKE",
          evidence      = Map(
            "amount"    -> f"${txn.amount}%.2f",
            "mean"      -> f"${stats.mean}%.2f",
            "stddev"    -> f"${stats.stddev}%.2f",
            "z_score"   -> f"$z%.2f",
            "threshold" -> zScoreThreshold.toString
          ),
          detectedAtMs = System.currentTimeMillis()
        ))
      }
    }

    statsState.update(stats.update(txn.amount))
  }

  private def checkGeoTravel(txn: ScoredTransaction, out: Collector[AnomalyEvent]): Unit = {
    if (txn.latitude == 0.0 && txn.longitude == 0.0) return

    Option(lastLocationState.value()) match {
      case Some((prevLat, prevLon, prevTimeMs)) if prevTimeMs > 0 =>
        val distanceKm    = haversineKm(prevLat, prevLon, txn.latitude, txn.longitude)
        val elapsedHours  = math.max((txn.eventTimeMs - prevTimeMs) / 3600000.0, 1e-9)
        val speedKmh      = distanceKm / elapsedHours

        if (speedKmh > maxSpeedKmh) {
          log.debug(s"GEO_TRAVEL flag: account=${txn.accountId} speed=${speedKmh} km/h")
          out.collect(AnomalyEvent(
            transactionId = txn.transactionId,
            accountId     = txn.accountId,
            reasonCode    = "GEO_TRAVEL",
            evidence      = Map(
              "distance_km"       -> f"$distanceKm%.1f",
              "elapsed_hours"     -> f"$elapsedHours%.3f",
              "implied_speed_kmh" -> f"$speedKmh%.1f",
              "max_speed_kmh"     -> maxSpeedKmh.toString,
              "prev_lat"          -> prevLat.toString,
              "prev_lon"          -> prevLon.toString,
              "curr_lat"          -> txn.latitude.toString,
              "curr_lon"          -> txn.longitude.toString
            ),
            detectedAtMs = System.currentTimeMillis()
          ))
        }
      case _ =>
    }

    lastLocationState.update((txn.latitude, txn.longitude, txn.eventTimeMs))
  }

  private def haversineKm(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val R    = 6371.0
    val dLat = toRadians(lat2 - lat1)
    val dLon = toRadians(lon2 - lon1)
    val a    = sin(dLat / 2) * sin(dLat / 2) +
               cos(toRadians(lat1)) * cos(toRadians(lat2)) * sin(dLon / 2) * sin(dLon / 2)
    R * 2 * atan2(sqrt(a), sqrt(1 - a))
  }
}
