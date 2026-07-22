package org.apache.spark.sql.sparkplug

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.config.ConfigBuilder

object Config {

  var lastActive = System.currentTimeMillis()
  var externalShutdown = false

  def updateLastActive() = {
    lastActive = System.currentTimeMillis()
  }

  // The auth token that must be used by the client to connect
  val SPARK_PLUG_CALLBACK =
    ConfigBuilder("spark.plug.callback")
      .stringConf
      .createWithDefaultFunction { () =>
        throw new IllegalArgumentException("Proxy callback must be provided")
      }

  // How long after no activity do we kill the session
  val SPARK_PLUG_IDLE_TIMEOUT =
    ConfigBuilder("spark.plug.idle.timeout")
      .timeConf(TimeUnit.SECONDS)
      .createOptional
}
