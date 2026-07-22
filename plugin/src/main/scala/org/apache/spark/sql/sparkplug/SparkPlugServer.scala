package org.apache.spark.sql.sparkplug

import org.apache.spark.sql.connect.service.SparkConnectServer

/**
 * Wrapper around the built-in SparkConnectServer. Spark does not currently support running
 * that class directly in cluster mode, so this just bypasses that limiation.
 */
object SparkPlugServer {
  def main(args: Array[String]): Unit = {
    SparkConnectServer.main(args)
  }
}
