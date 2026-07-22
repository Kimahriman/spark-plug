package org.apache.spark.sql.sparkplug

import java.net.http.{HttpClient, HttpRequest}
import java.net.http.HttpResponse.BodyHandlers
import java.net.{Socket, URI}
import java.security.cert.X509Certificate
import java.security.SecureRandom
import java.time.Duration
import java.time.temporal.ChronoUnit.SECONDS
import javax.net.ssl.{SSLContext, SSLEngine, X509ExtendedTrustManager}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.service.{SparkConnectService, SparkListenerConnectServiceEnd, SparkListenerConnectServiceStarted}

class SparkPlugListener(conf: SparkConf) extends SparkListener with Logging {

  val callbackAddr = conf.get(Config.SPARK_PLUG_CALLBACK)

  val token = conf.get("spark.connect.authenticate.token")

  lazy val authorizationHeader = s"Bearer $token"

  // val trustManager: X509ExtendedTrustManager = new X509ExtendedTrustManager() {

  //   override def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = {}

  //   override def getAcceptedIssuers(): Array[X509Certificate] = Array.empty

  //   override def checkClientTrusted(chain: Array[X509Certificate], authType: String, socket: Socket): Unit = {}

  //   override def checkServerTrusted(chain: Array[X509Certificate], authType: String, socket: Socket): Unit = {}

  //   override def checkClientTrusted(chain: Array[X509Certificate], authType: String, engine: SSLEngine): Unit = {}

  //   override def checkServerTrusted(chain: Array[X509Certificate], authType: String, engine: SSLEngine): Unit = {}

  //   override def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = {}
  // }

  // val sslContext = SSLContext.getInstance("TLS")
  // sslContext.init(null, Array(trustManager), new SecureRandom())

  // lazy val client = HttpClient.newBuilder().sslContext(sslContext).build()

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    Config.updateLastActive()

    event match {
      case SparkListenerConnectServiceStarted(hostAddress, bindingPort, _) =>
        val connectUri = s"$hostAddress:$bindingPort"

        logInfo(s"Connect service started on $connectUri")

        val appId = SparkContext.getOrCreate().applicationId
        
        val request = HttpRequest.newBuilder()
          .uri(URI.create(s"$callbackAddr/callback"))
          .timeout(Duration.of(10, SECONDS))
          .setHeader("Authorization", authorizationHeader)
          .setHeader("Content-type", "application/json")
          .POST(HttpRequest.BodyPublishers.ofString({
            s"{\"address\": \"$connectUri\", \"application_id\": \"$appId\"}"
          }))
          .build()

        logInfo(s"Sending callback info to ${request.uri()}")

        try {
          val client = HttpClient.newHttpClient()
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
      case _: SparkListenerConnectServiceEnd if !Config.externalShutdown =>
        val request = HttpRequest.newBuilder()
          .uri(URI.create(s"$callbackAddr/callback"))
          .timeout(Duration.of(10, SECONDS))
          .setHeader("Authorization", authorizationHeader)
          .setHeader("Content-type", "application/json")
          .DELETE()
          .build()

        logInfo(s"Sending callback delete to ${request.uri()}")

        try {
          val client = HttpClient.newHttpClient()
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


  val timeoutThread = conf.get(Config.SPARK_PLUG_IDLE_TIMEOUT).map { timeout =>
    val timeoutThread = new Thread(new Runnable() {
      override def run(): Unit = {
        while (true) {
          if (SparkConnectService.listActiveExecutions.isRight) {
            Config.updateLastActive()
          } else if (Config.lastActive < (System.currentTimeMillis() - timeout * 1000)) {
            logInfo(s"Application has been idle for $timeout seconds, shutting down")
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
