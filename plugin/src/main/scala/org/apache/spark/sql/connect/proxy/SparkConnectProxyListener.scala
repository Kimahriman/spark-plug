package org.apache.spark.sql.connect.proxy

import java.net.http.HttpRequest
import java.net.URI
import java.time.Duration

import java.time.temporal.ChronoUnit.SECONDS

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.connect.service.SparkConnectService
import org.apache.spark.sql.connect.service.SparkListenerConnectServiceStarted
import java.net.http.HttpClient
import java.net.http.HttpResponse.BodyHandlers
import org.apache.spark.SparkException
import org.apache.spark.SparkConf
import org.apache.spark.sql.connect.service.SparkListenerConnectServiceEnd
import org.apache.spark.sql.connect.config.Connect

class SparkConnectProxyListener(conf: SparkConf) extends SparkListener with Logging {

  val callbackAddr = conf.get(Config.SPARK_CONNECT_PROXY_CALLBACK)

  val token = conf.get("spark.connect.authenticate.token")

  lazy val authorizationHeader = s"Bearer $token"

  lazy val client = HttpClient.newHttpClient()

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    Config.updateLastActive()

    event match {
      case SparkListenerConnectServiceStarted(hostAddress, bindingPort, _) =>
        val connectUri = s"$hostAddress:$bindingPort"

        logInfo(s"Connect service started on $connectUri")
        
        val request = HttpRequest.newBuilder()
          .uri(URI.create(s"$callbackAddr/callback"))
          .timeout(Duration.of(10, SECONDS))
          .setHeader("Authorization", authorizationHeader)
          .setHeader("Content-type", "application/json")
          .POST(HttpRequest.BodyPublishers.ofString(s"{\"address\": \"$connectUri\"}"))
          .build()

        logInfo(s"Sending callback info to ${request.uri()}")

        try {
          val response = client.send(request, BodyHandlers.discarding())

          if (response.statusCode() != 200) {
            logError(s"Bad status code returned from proxy server: ${response.statusCode()}")
            SparkConnectService.stop()
          }
        }
        catch {
          case e: Throwable => {
            logError(s"Failed to send connect address to callback", e)
            SparkConnectService.stop()
          }
        }
      case _: SparkListenerConnectServiceEnd =>
        val request = HttpRequest.newBuilder()
          .uri(URI.create(s"$callbackAddr/callback"))
          .timeout(Duration.of(10, SECONDS))
          .setHeader("Authorization", authorizationHeader)
          .setHeader("Content-type", "application/json")
          .DELETE()
          .build()

        logInfo(s"Sending callback delete to ${request.uri()}")

        try {
          val response = client.send(request, BodyHandlers.discarding())

          if (response.statusCode() != 200) {
            logError(s"Bad status code returned from proxy server: ${response.statusCode()}")
          }
        }
        catch {
          case e: Throwable => {
            logError(s"Failed to send connect address to callback", e)
          }
        }
      case _ => ()
    }
  }


  val timeoutThread = conf.get(Config.SPARK_CONNECT_PROXY_IDLE_TIMEOUT).map { timeout =>
    val timeoutThread = new Thread(new Runnable() {
      override def run(): Unit = {
        while (true) {
          if (Config.lastActive < (System.currentTimeMillis() - timeout * 1000)) {
            SparkConnectService.stop()
            return
          }

          Thread.sleep(60000)
        }
      }
    })
    timeoutThread.setDaemon(true)
    timeoutThread.start()
    timeoutThread
  }
}
