package com.fraudpipeline.utils

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.{Span, SpanKind, StatusCode, Tracer}
import io.opentelemetry.context.propagation.{TextMapGetter, TextMapSetter}
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Header
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets

// Initialise OTel SDK once per JVM via the autoconfigure extension.
// Set env vars: OTEL_EXPORTER_OTLP_ENDPOINT and OTEL_SERVICE_NAME before the job starts.
object Telemetry {
  private val log = LoggerFactory.getLogger(getClass)

  @volatile private var _sdk: OpenTelemetrySdk = _

  def init(): Unit = synchronized {
    if (_sdk == null) {
      try {
        _sdk = AutoConfiguredOpenTelemetrySdk.initialize().getOpenTelemetrySdk
        log.info("OpenTelemetry SDK initialised (autoconfigure)")
      } catch {
        case e: Exception =>
          log.warn("OTel init failed — tracing disabled: {}", e.getMessage)
      }
    }
  }

  def tracer(instrumentationName: String): Tracer =
    GlobalOpenTelemetry.getTracer(instrumentationName)

  // Propagate trace context via Kafka record headers
  object KafkaPropagator {
    private val setter: TextMapSetter[ProducerRecord[_, _]] =
      (carrier, key, value) =>
        carrier.headers().add(key, value.getBytes(StandardCharsets.UTF_8))

    private val getter: TextMapGetter[ConsumerRecord[_, _]] =
      new TextMapGetter[ConsumerRecord[_, _]] {
        override def keys(carrier: ConsumerRecord[_, _]): java.lang.Iterable[String] = {
          import scala.collection.JavaConverters._
          carrier.headers().toArray.map(_.key()).toList.asJava
        }
        override def get(carrier: ConsumerRecord[_, _], key: String): String = {
          val h: Header = carrier.headers().lastHeader(key)
          if (h == null) null else new String(h.value(), StandardCharsets.UTF_8)
        }
      }

    def inject(record: ProducerRecord[_, _], span: Span): Unit = {
      GlobalOpenTelemetry.getPropagators.getTextMapPropagator
        .inject(io.opentelemetry.context.Context.current().`with`(span), record, setter)
    }

    def extract(record: ConsumerRecord[_, _]): io.opentelemetry.context.Context =
      GlobalOpenTelemetry.getPropagators.getTextMapPropagator
        .extract(io.opentelemetry.context.Context.current(), record, getter)
  }

  // Convenience: wrap a block in a span
  def withSpan[T](tracer: Tracer, name: String, kind: SpanKind = SpanKind.INTERNAL)(f: Span => T): T = {
    val span = tracer.spanBuilder(name).setSpanKind(kind).startSpan()
    val scope = span.makeCurrent()
    try {
      val result = f(span)
      span.setStatus(StatusCode.OK)
      result
    } catch {
      case e: Exception =>
        span.setStatus(StatusCode.ERROR, e.getMessage)
        span.recordException(e)
        throw e
    } finally {
      scope.close()
      span.end()
    }
  }
}
