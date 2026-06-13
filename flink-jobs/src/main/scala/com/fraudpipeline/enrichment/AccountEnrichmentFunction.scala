package com.fraudpipeline.enrichment

import com.fraudpipeline.utils._
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

private object AccountTypes {
  type IN1 = (RawTransaction, Option[MerchantProfile])
  type IN2 = String
  type OUT = (RawTransaction, Option[MerchantProfile], Option[AccountProfile])
  type ROCtx = BroadcastProcessFunction[IN1, IN2, OUT]#ReadOnlyContext
  type Ctx   = BroadcastProcessFunction[IN1, IN2, OUT]#Context
}

class AccountEnrichmentFunction
  extends BroadcastProcessFunction[
    (RawTransaction, Option[MerchantProfile]),
    String,
    (RawTransaction, Option[MerchantProfile], Option[AccountProfile])
  ] {

  import AccountTypes._

  private val log = LoggerFactory.getLogger(getClass)

  override def processElement(
    pair: (RawTransaction, Option[MerchantProfile]),
    ctx : ROCtx,
    out : Collector[(RawTransaction, Option[MerchantProfile], Option[AccountProfile])]
  ): Unit = {
    val state   = ctx.getBroadcastState(EnrichScoreJob.accountStateDesc)
    val account = Option(state.get(pair._1.accountId))
    if (account.isEmpty) log.debug(s"Account lookup miss: ${pair._1.accountId}")
    out.collect((pair._1, pair._2, account))
  }

  override def processBroadcastElement(
    json: String,
    ctx : Ctx,
    out : Collector[(RawTransaction, Option[MerchantProfile], Option[AccountProfile])]
  ): Unit = {
    try {
      val profile = JsonSerde.fromJson[AccountProfile](json)
      ctx.getBroadcastState(EnrichScoreJob.accountStateDesc).put(profile.accountId, profile)
    } catch {
      case e: Exception =>
        log.warn(s"Failed to parse account profile — ${e.getMessage}: $json")
    }
  }
}
