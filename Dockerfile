FROM rust:1.87 AS rust-builder

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

FROM cgr.dev/chainguard/wolfi-base AS base

RUN apk add openjdk-17-jre wget bash

WORKDIR /opt/spark-connect-proxy

COPY --from=rust-builder /usr/local/cargo/bin/spark-connect-proxy /opt/spark-connect-proxy/
COPY --from=java-builder /usr/src/app/plugin/target/scala-2.13/spark-connect-proxy*.jar /opt/spark-connect-proxy/

CMD ["/opt/spark-connect-proxy/spark-connect-proxy"]

FROM cgr.dev/chainguard/wolfi-base AS spark-cache

RUN apk add wget

ARG SPARK_VERSION=4.0.0

RUN wget -q https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt && \
    rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz

FROM base

ARG SPARK_VERSION=4.0.0

COPY --from=spark-cache /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark

ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk