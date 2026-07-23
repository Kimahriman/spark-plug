FROM rust:1 AS rust-builder

WORKDIR /usr/src/app

COPY server /usr/src/app/

RUN --mount=type=cache,target=/usr/src/app/target/ \
    --mount=type=cache,target=/usr/local/cargo/registry/ \
    cargo install --path .

FROM chainguard/wolfi-base AS java-builder

RUN apk update && \
    apk add openjdk-17 sbt

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV PATH=$PATH:$JAVA_HOME/bin

WORKDIR /usr/src/app

COPY build.sbt /usr/src/app/
COPY plugin /usr/src/app/plugin
COPY project /usr/src/app/project

RUN --mount=type=cache,target=/root/.cache/coursier \
    --mount=type=cache,target=/root/.sbt \
    sbt package

FROM chainguard/wolfi-base as base

RUN apk update && \
    apk add openjdk-17 wget python-3.12-base python-3.14-base uv

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk

WORKDIR /opt/spark-plug

ARG VERSION=0.1.0
ARG PLUGIN_JAR=spark-plug_2.13-${VERSION}.jar

COPY --from=rust-builder /usr/local/cargo/bin/spark-plug /opt/spark-plug/
COPY --from=java-builder /usr/src/app/plugin/target/scala-2.13/${PLUGIN_JAR} /opt/spark-plug/spark-plug_2.13.jar

ENV SPARK_PLUG_PLUGIN_PATH=/opt/spark-plug/spark-plug_2.13.jar

CMD ["/opt/spark-plug/spark-plug"]

FROM base

ARG SPARK_VERSION=4.2.0

RUN wget -q https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark