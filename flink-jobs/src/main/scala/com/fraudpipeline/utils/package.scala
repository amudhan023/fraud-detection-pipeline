package com.fraudpipeline

package object utils {

  case class RawTransaction(
    transactionId : String,
    accountId     : String,
    merchantId    : String,
    amount        : Double,
    currency      : String,
    latitude      : Double,
    longitude     : Double,
    eventTimeMs   : Long,
    op            : String   // 'c'=create 'u'=update 'd'=delete
  )

  case class MerchantProfile(
    merchantId    : String,
    name          : String,
    category      : String,
    riskCategory  : String   // LOW / MEDIUM / HIGH
  )

  case class AccountProfile(
    accountId   : String,
    tier        : String,    // STANDARD / PREMIUM / VIP
    country     : String,
    riskScore   : Double
  )

  case class ScoredTransaction(
    transactionId  : String,
    accountId      : String,
    merchantId     : String,
    amount         : Double,
    currency       : String,
    latitude       : Double,
    longitude      : Double,
    eventTimeMs    : Long,
    merchantCategory : String,
    merchantRisk   : String,
    accountTier    : String,
    accountCountry : String,
    riskScore      : Double,
    scoreBreakdown : Map[String, Double]
  )

  case class AnomalyEvent(
    transactionId : String,
    accountId     : String,
    reasonCode    : String,
    evidence      : Map[String, String],
    detectedAtMs  : Long
  )

  case class SpendAggregate(
    windowStart    : Long,
    windowEnd      : Long,
    dimension      : String,
    dimensionValue : String,
    totalAmount    : Double,
    txnCount       : Long
  )

  // Welford online algorithm state for z-score
  case class RunningStats(count: Long, mean: Double, m2: Double) {
    def update(x: Double): RunningStats = {
      val n = count + 1
      val delta = x - mean
      val newMean = mean + delta / n
      val delta2 = x - newMean
      RunningStats(n, newMean, m2 + delta * delta2)
    }

    def variance: Double = if (count < 2) 0.0 else m2 / (count - 1)
    def stddev: Double   = math.sqrt(variance)

    def zScore(x: Double): Double =
      if (stddev == 0.0) 0.0 else (x - mean) / stddev
  }

  object RunningStats {
    val empty: RunningStats = RunningStats(0L, 0.0, 0.0)
  }
}
