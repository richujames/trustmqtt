# FlashMQ 1.27.x + the native TrustMQTT adapter. FlashMQ recommends building
# plugins against the same OS/library versions as the broker package, so both
# are installed and compiled in this Debian Bookworm image.
FROM debian:bookworm-slim

ARG FLASHMQ_VERSION=1.27.1

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl git build-essential cmake pkg-config \
        libhiredis-dev libcjson-dev libssl-dev && \
    # The upstream package requires a systemctl command even in containers.
    # PID 1 remains FlashMQ; the no-op shim only satisfies package lifecycle hooks.
    ln -s /bin/true /usr/bin/systemctl && \
    curl -fsSL https://www.flashmq.org/flashmq-repo.gpg \
        -o /usr/share/keyrings/flashmq-repo.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/flashmq-repo.gpg] http://repo.flashmq.org/apt bookworm main" \
        > /etc/apt/sources.list.d/flashmq.list && \
    apt-get update && \
    FLASHMQ_PACKAGE_VERSION="$(apt-cache policy flashmq | awk '/Candidate:/ {print $2}')" && \
    case "$FLASHMQ_PACKAGE_VERSION" in \
        "${FLASHMQ_VERSION}"-*) ;; \
        *) echo "FlashMQ package/header version mismatch: $FLASHMQ_PACKAGE_VERSION" >&2; exit 1 ;; \
    esac && \
    apt-get install -y --no-install-recommends "flashmq=${FLASHMQ_PACKAGE_VERSION}" && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt
RUN git clone --branch "v${FLASHMQ_VERSION}" --depth 1 \
        https://github.com/halfgaar/FlashMQ.git flashmq-source

WORKDIR /build/plugin
COPY plugin/include/ include/
COPY plugin/src/ src/
COPY plugin/CMakeLists.txt .

RUN cmake -S . -B build \
        -DCMAKE_BUILD_TYPE=Release \
        -DFLASHMQ_INCLUDE_DIR=/opt/flashmq-source && \
    cmake --build build --target trustmqtt_flashmq_plugin -j"$(nproc)"

RUN install -d /etc/flashmq /var/lib/flashmq /usr/lib/flashmq && \
    install -m 0755 build/trustmqtt_flashmq_plugin.so \
        /usr/lib/flashmq/trustmqtt_flashmq_plugin.so

COPY docker/flashmq/flashmq.conf /etc/flashmq/flashmq.conf
COPY docker/mosquitto/acl.conf /etc/flashmq/acl.conf

EXPOSE 1883
ENTRYPOINT ["flashmq"]
CMD ["--config-file", "/etc/flashmq/flashmq.conf"]
