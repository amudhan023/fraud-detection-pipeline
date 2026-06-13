package com.fraudpipeline.enrichment

import com.fraudpipeline.utils._
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

// Type aliases for the Java inner-class types (Scala requires type-projection syntax)
private object MerchantTypes {
  type IN1 = RawTransaction
  type IN2 = String
  type OUT = (RawTransaction, Option[MerchantProfile])
  type ROCtx = BroadcastProcessFunction[IN1, IN2, OUT]#ReadOnlyContext
  type Ctx   = BroadcastProcessFunction[IN1, IN2, OUT]#Context
}

class MerchantEnrichmentFunction
  extends BroadcastProcessFunction[
    RawTransaction,
    String,
    (RawTransaction, Option[MerchantProfile])
  ] {

  import MerchantTypes._

  private val log = LoggerFactory.getLogger(getClass)

  override def processElement(
    txn : RawTransaction,
    ctx : ROCtx,
    out : Collector[(RawTransaction, Option[MerchantProfile])]
  ): Unit = {
    val state    = ctx.getBroadcastState(EnrichScoreJob.merchantStateDesc)
    val merchant = Option(state.get(txn.merchantId))
    if (merchant.isEmpty) log.debug(s"Merchant lookup miss: ${txn.merchantId}")
    out.collect((txn, merchant))
  }

  override def processBroadcastElement(
    json: String,
    ctx : Ctx,
    out : Collector[(RawTransaction, Option[MerchantProfile])]
  ): Unit = {
    try {
      val profile = JsonSerde.fromJson[MerchantProfile](json)
      ctx.getBroadcastState(EnrichScoreJob.merchantStateDesc).put(profile.merchantId, profile)
    } catch {
      case e: Exception =>
        log.warn(s"Failed to parse merchant profile — ${e.getMessage}: $json")
    }
  }
}
