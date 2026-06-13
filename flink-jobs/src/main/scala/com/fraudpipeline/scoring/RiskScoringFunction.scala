package com.fraudpipeline.scoring

import com.fraudpipeline.utils._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

// Type alias for the parameterised context type — needed for Scala type-projection syntax
private object RSFTypes {
  type KPF    = KeyedProcessFunction[String, (RawTransaction, Option[MerchantProfile], Option[AccountProfile]), ScoredTransaction]
  type Ctx    = KPF#Context
}

class RiskScoringFunction
  extends KeyedProcessFunction[
    String,
    (RawTransaction, Option[MerchantProfile], Option[AccountProfile]),
    ScoredTransaction
  ] {

  import RSFTypes._

  @transient private var avgAmountState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit =
    avgAmountState = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("account-avg-amount", classOf[Double])
    )

  override def processElement(
    tuple: (RawTransaction, Option[MerchantProfile], Option[AccountProfile]),
    ctx  : Ctx,
    out  : Collector[ScoredTransaction]
  ): Unit = {
    val (txn, merchantOpt, accountOpt) = tuple

    val merchant = merchantOpt.getOrElse(MerchantProfile(txn.merchantId, "Unknown", "UNKNOWN", "MEDIUM"))
    val account  = accountOpt.getOrElse(AccountProfile(txn.accountId, "STANDARD", "US", 0.3))

    val breakdown = scala.collection.mutable.Map[String, Double]()

    val amountScore   = math.min(math.log1p(txn.amount) / math.log1p(10000.0), 1.0) * 0.25
    breakdown("amount") = amountScore

    val merchantScore = merchant.riskCategory match {
      case "HIGH"   => 0.35
      case "MEDIUM" => 0.15
      case _        => 0.05
    }
    breakdown("merchant_risk") = merchantScore

    val accountScore = account.riskScore * 0.20
    breakdown("account_baseline") = accountScore

    val hourUtc    = ((ctx.timestamp() / 3600000L) % 24).toInt
    val timeScore  = if (hourUtc >= 0 && hourUtc < 6) 0.10 else 0.0
    breakdown("time_of_day") = timeScore

    val crossBorderScore = if (merchant.riskCategory == "HIGH" && account.country != "US") 0.10 else 0.0
    breakdown("cross_border") = crossBorderScore

    val totalScore = math.min(breakdown.values.sum, 1.0)

    val prevAvg = Option(avgAmountState.value()).getOrElse(txn.amount)
    avgAmountState.update(prevAvg * 0.9 + txn.amount * 0.1)

    out.collect(ScoredTransaction(
      transactionId    = txn.transactionId,
      accountId        = txn.accountId,
      merchantId       = txn.merchantId,
      amount           = txn.amount,
      currency         = txn.currency,
      latitude         = txn.latitude,
      longitude        = txn.longitude,
      eventTimeMs      = txn.eventTimeMs,
      merchantCategory = merchant.category,
      merchantRisk     = merchant.riskCategory,
      accountTier      = account.tier,
      accountCountry   = account.country,
      riskScore        = totalScore,
      scoreBreakdown   = breakdown.toMap
    ))
  }
}
