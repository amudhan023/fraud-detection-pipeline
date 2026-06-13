package com.fraudpipeline.utils

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.metrics.Counter

// Thin wrapper registering custom Flink metrics that the Prometheus reporter
// scrapes automatically once flink-metrics-prometheus is on the classpath.
class PipelineMetrics(ctx: RuntimeContext) extends Serializable {
  private val group = ctx.getMetricGroup.addGroup("fraud_pipeline")

  val eventsProcessed  : Counter = group.counter("events_processed_total")
  val lateEventsDropped: Counter = group.counter("late_events_dropped_total")
  val anomaliesDetected: Counter = group.counter("anomalies_detected_total")
  val enrichmentMisses : Counter = group.counter("enrichment_misses_total")
  val velocityFlags    : Counter = group.counter("velocity_flags_total")
  val amountSpikeFlags : Counter = group.counter("amount_spike_flags_total")
  val geoTravelFlags   : Counter = group.counter("geo_travel_flags_total")
}
