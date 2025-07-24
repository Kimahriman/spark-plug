FROM rust:1.88 AS rust-builder

WORKDIR /usr/src/app

COPY server /usr/src/app/

RUN --mount=type=cache,target=/usr/src/app/target/ \
    --mount=type=cache,target=/usr/local/cargo/registry/ \
    cargo install --path .

FROM eclipse-temurin:17 AS java-builder

ARG SBT_VERSION=1.10.11

RUN wget -q https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz && \
    tar -xf sbt-${SBT_VERSION}.tgz -C /opt && \
    /opt/sbt/bin/sbt --version

WORKDIR /usr/src/app

COPY build.sbt /usr/src/app/
COPY plugin /usr/src/app/plugin
COPY project /usr/src/app/project

RUN --mount=type=cache,target=/root/.cache/coursier \
    --mount=type=cache,target=/root/.sbt \
    /opt/sbt/bin/sbt package

FROM ubuntu:noble AS base

RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless wget && \
    ln -s /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /usr/lib/jvm/java-17-openjdk

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk

WORKDIR /opt/spark-connect-proxy

ARG VERSION=0.1.0
ARG PLUGIN_JAR=spark-connect-proxy_2.13-${VERSION}.jar

COPY --from=rust-builder /usr/local/cargo/bin/spark-connect-proxy /opt/spark-connect-proxy/
COPY --from=java-builder /usr/src/app/plugin/target/scala-2.13/${PLUGIN_JAR} /opt/spark-connect-proxy/spark-connect-proxy_2.13.jar

ENV CONNECT_PROXY_PLUGIN_PATH=/opt/spark-connect-proxy/spark-connect-proxy_2.13.jar

CMD ["/opt/spark-connect-proxy/spark-connect-proxy"]

FROM ubuntu:noble AS spark-cache

ARG SPARK_VERSION=4.0.0

RUN apt-get update && \
    apt-get install -y wget

RUN wget -q https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt && \
    rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz

FROM base

ARG SPARK_VERSION=4.0.0

COPY --from=spark-cache /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark

ENV SPARK_HOME=/opt/spark