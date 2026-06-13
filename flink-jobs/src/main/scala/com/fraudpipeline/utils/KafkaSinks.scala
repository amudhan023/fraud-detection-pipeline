package com.fraudpipeline.utils

import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.kafka.clients.producer.ProducerRecord

import java.nio.charset.StandardCharsets

// Serialize T → JSON with a String key, publishing to a fixed topic.
// KafkaSinkContext is a nested interface: KafkaRecordSerializationSchema.KafkaSinkContext
class JsonKafkaSink[T](topic: String, keyFn: T => String)
  extends KafkaRecordSerializationSchema[T] {

  override def serialize(
    element  : T,
    context  : KafkaRecordSerializationSchema.KafkaSinkContext,
    timestamp: java.lang.Long
  ): ProducerRecord[Array[Byte], Array[Byte]] =
    new ProducerRecord[Array[Byte], Array[Byte]](
      topic,
      keyFn(element).getBytes(StandardCharsets.UTF_8),
      JsonSerde.toJson(element).getBytes(StandardCharsets.UTF_8)
    )
}

object KafkaSinks {
  def jsonSink[T](bootstrapServers: String, topic: String, keyFn: T => String): KafkaSink[T] =
    KafkaSink.builder[T]()
      .setBootstrapServers(bootstrapServers)
      .setRecordSerializer(new JsonKafkaSink[T](topic, keyFn))
      .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()
}
