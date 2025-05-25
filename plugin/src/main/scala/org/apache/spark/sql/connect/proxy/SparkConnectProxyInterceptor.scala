package org.apache.spark.sql.connect.proxy

import org.apache.spark.SparkContext
import org.sparkproject.connect.grpc.ServerInterceptor
import org.sparkproject.connect.grpc.{Metadata, ServerCall, ServerCallHandler}
import org.sparkproject.connect.grpc.ServerCall.Listener
import org.sparkproject.connect.grpc.Status
import org.apache.spark.sql.connect.proxy.Config.SPARK_CONNECT_PROXY_IDLE_TIMEOUT
import org.apache.spark.sql.connect.service.SparkConnectService

class SparkConnectProxyInterceptor extends ServerInterceptor {

  val sparkContext = SparkContext.getActive.get

  val token = sparkContext.getConf.get(Config.SPARK_CONNECT_PROXY_TOKEN)

  val authHeader = Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER)
  val proxyMessageHeader = Metadata.Key.of("X-Connect-Proxy", Metadata.ASCII_STRING_MARSHALLER)

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT,RespT],
      metadata: Metadata,
      next: ServerCallHandler[ReqT,RespT]
  ): Listener[ReqT] = {
    val authHeaderValue = metadata.get(authHeader);

    if (authHeaderValue == null) {
      val status = Status.UNAUTHENTICATED.withDescription("No authentication token provided");
      call.close(status, new Metadata())
      new Listener[ReqT]() {}
    } else if (authHeaderValue != s"Bearer $token") {
      val status = Status.UNAUTHENTICATED.withDescription("Invalid authentication token");
      call.close(status, new Metadata())
      new Listener[ReqT]() {}
    } else {
      Option(metadata.get(proxyMessageHeader)) match {
        case Some("stop") =>
          SparkConnectService.stop()
          call.close(Status.OK, new Metadata())
          new Listener[ReqT]() {}
        case Some(message) =>
          call.close(Status.INVALID_ARGUMENT, new Metadata())
          new Listener[ReqT]() {}
        case None =>
          Config.updateLastActive()
          next.startCall(call, metadata)
      }
    }
  }
}
