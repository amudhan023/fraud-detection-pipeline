package com.fraudpipeline.utils

import com.typesafe.config.{Config, ConfigFactory}

object PipelineConfig {
  lazy val root: Config = ConfigFactory.load("application.conf")
  lazy val kafka: Config   = root.getConfig("pipeline.kafka")
  lazy val flink: Config   = root.getConfig("pipeline.flink")
  lazy val postgres: Config = root.getConfig("pipeline.postgres")
  lazy val anomaly: Config = root.getConfig("pipeline.anomaly")
  lazy val otel: Config    = root.getConfig("pipeline.otel")

  object Topics {
    def raw: String       = kafka.getString("topics.transactions-raw")
    def scored: String    = kafka.getString("topics.transactions-scored")
    def anomalies: String = kafka.getString("topics.transactions-anomalies")
    def spend: String     = kafka.getString("topics.analytics-spend")
    def merchants: String = kafka.getString("topics.enrichment-merchants")
    def accounts: String  = kafka.getString("topics.enrichment-accounts")
  }
}
