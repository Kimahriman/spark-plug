FROM rust:1.87 AS rust-builder

WORKDIR /usr/src/app

COPY server /usr/src/app/

RUN --mount=type=cache,target=/usr/src/app/target/ \
    --mount=type=cache,target=/usr/local/cargo/registry/ \
    cargo install --path .

FROM eclipse-temurin:17 AS java-builder

RUN wget -q https://github.com/sbt/sbt/releases/download/v1.10.11/sbt-1.10.11.tgz && \
    tar -xf sbt-1.10.11.tgz -C /opt && \
    /opt/sbt/bin/sbt --version

WORKDIR /usr/src/app

COPY build.sbt /usr/src/app/
COPY plugin /usr/src/app/plugin
COPY project /usr/src/app/project

RUN --mount=type=cache,target=/root/.cache/coursier \
    --mount=type=cache,target=/root/.sbt \
    /opt/sbt/bin/sbt package

FROM debian:bookworm-slim AS base

RUN apt-get update && apt-get install -y wget openjdk-17-jre-headless

WORKDIR /opt/spark-connect-proxy

COPY --from=rust-builder /usr/local/cargo/bin/spark-connect-proxy /opt/spark-connect-proxy/
COPY --from=java-builder /usr/src/app/plugin/target/scala-2.13/spark-connect-proxy*.jar /opt/spark-connect-proxy/

CMD ["/opt/spark-connect-proxy/spark-connect-proxy"]

FROM base

ARG SPARK_VERSION=4.0.0

RUN wget -q https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt && \
    rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark-${SPARK_VERSION}-bin-hadoop3
