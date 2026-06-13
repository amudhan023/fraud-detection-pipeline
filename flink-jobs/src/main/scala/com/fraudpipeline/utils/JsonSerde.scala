package com.fraudpipeline.utils

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.nio.charset.StandardCharsets

object JsonSerde {
  @transient private lazy val mapper: ObjectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new JavaTimeModule())
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def toJson[T](value: T): String = mapper.writeValueAsString(value)
  def fromJson[T: Manifest](json: String): T =
    mapper.readValue(json, implicitly[Manifest[T]].runtimeClass.asInstanceOf[Class[T]])

  // ── Debezium envelope unwrapper ──────────────────────────────────────────
  // Debezium CDC record: {"before":..., "after":{...}, "op":"c", "ts_ms":...}
  def unwrapDebezium(json: String): Option[String] = {
    try {
      val node = mapper.readTree(json)
      val op   = if (node.has("op")) node.get("op").asText() else "c"
      if (op == "d") return None  // skip deletes
      val after = node.get("after")
      if (after == null || after.isNull) None
      else Some(mapper.writeValueAsString(after))
    } catch {
      case _: Exception => None
    }
  }

  // ── Schema for RawTransaction from Debezium CDC ─────────────────────────
  def parseDebeziumTransaction(json: String): Option[RawTransaction] = {
    unwrapDebezium(json).flatMap { afterJson =>
      try {
        val node = mapper.readTree(afterJson)
        def str(f: String) = if (node.has(f) && !node.get(f).isNull) node.get(f).asText() else ""
        def dbl(f: String) = if (node.has(f) && !node.get(f).isNull) node.get(f).asDouble() else 0.0
        def lng(f: String) = if (node.has(f) && !node.get(f).isNull) node.get(f).asLong() else 0L

        val raw = mapper.readTree(json)
        val op  = if (raw.has("op")) raw.get("op").asText() else "c"

        Some(RawTransaction(
          transactionId = str("transaction_id"),
          accountId     = str("account_id"),
          merchantId    = str("merchant_id"),
          amount        = dbl("amount"),
          currency      = if (str("currency").nonEmpty) str("currency") else "USD",
          latitude      = dbl("latitude"),
          longitude     = dbl("longitude"),
          eventTimeMs   = {
            val et = lng("event_time")
            if (et > 0) et else System.currentTimeMillis()
          },
          op = op
        ))
      } catch {
        case _: Exception => None
      }
    }
  }

  // ── Generic Flink serialization schemas ─────────────────────────────────

  def serializationSchema[T: Manifest]: SerializationSchema[T] =
    (element: T) => toJson(element).getBytes(StandardCharsets.UTF_8)

  def deserializationSchema[T: Manifest](
    typeInfo: TypeInformation[T]
  ): DeserializationSchema[T] = new DeserializationSchema[T] {
    override def deserialize(message: Array[Byte]): T =
      fromJson[T](new String(message, StandardCharsets.UTF_8))
    override def isEndOfStream(nextElement: T): Boolean = false
    override def getProducedType: TypeInformation[T] = typeInfo
  }
}
