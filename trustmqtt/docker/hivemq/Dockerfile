FROM maven:3.9.9-eclipse-temurin-21 AS builder
WORKDIR /build
COPY hivemq-extension/pom.xml .
COPY hivemq-extension/src src
RUN mvn -B package

FROM hivemq/hivemq-ce:2026.5
USER root
# TrustMQTT's explicit demo authenticator supplies scoped permissions instead.
RUN mkdir -p /opt/hivemq/extensions/hivemq-allow-all-extension && \
    touch /opt/hivemq/extensions/hivemq-allow-all-extension/DISABLED
COPY --from=builder /build/target/trustmqtt-hivemq.jar /opt/hivemq/extensions/trustmqtt/trustmqtt-hivemq.jar
COPY hivemq-extension/hivemq-extension.xml /opt/hivemq/extensions/trustmqtt/hivemq-extension.xml
COPY docker/hivemq/config.xml /opt/hivemq/conf/config.xml
RUN chown -R 10000:0 /opt/hivemq/extensions/trustmqtt /opt/hivemq/conf/config.xml
USER 10000
