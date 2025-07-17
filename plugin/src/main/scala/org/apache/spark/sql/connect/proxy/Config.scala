package org.apache.spark.sql.connect.proxy

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.config.ConfigBuilder

object Config {

  var lastActive = System.currentTimeMillis()
  var externalShutdown = false

  def updateLastActive() = {
    lastActive = System.currentTimeMillis()
  }

  // The auth token that must be used by the client to connect
  val SPARK_CONNECT_PROXY_CALLBACK =
    ConfigBuilder("spark.connect.proxy.callback")
      .stringConf
      .createWithDefaultFunction { () =>
        throw new IllegalArgumentException("Proxy callback must be provided")
      }

  // How long after no activity do we kill the session
  val SPARK_CONNECT_PROXY_IDLE_TIMEOUT =
    ConfigBuilder("spark.connect.proxy.idle.timeout")
      .timeConf(TimeUnit.SECONDS)
      .createOptional
}
