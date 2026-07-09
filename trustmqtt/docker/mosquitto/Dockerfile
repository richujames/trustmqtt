# TrustMQTT broker image — builds Eclipse Mosquitto 2.1.x from source (the
# distro/Alpine package is still pinned to 2.0.x at the time of writing, and
# the plugin needs MOSQ_EVT_CONNECT / MOSQ_EVT_CLIENT_OFFLINE from 2.1.0),
# then builds trustmqtt_plugin.so against the freshly built broker headers.
#
# Build context MUST be the repo root (docker-compose sets this), because we
# need both plugin/ and this Dockerfile in the build.

ARG MOSQUITTO_VERSION=2.1.0

##############################################
# Stage 1: build Mosquitto 2.1.x broker from source
##############################################
FROM alpine:3.19 AS mosquitto-builder
ARG MOSQUITTO_VERSION

RUN apk add --no-cache \
        build-base cmake pkgconfig git \
        openssl-dev libwebsockets-dev c-ares-dev util-linux-dev cjson-dev \
        sqlite-dev argon2-dev

WORKDIR /build
RUN git clone --branch "v${MOSQUITTO_VERSION}" --depth 1 \
        https://github.com/eclipse/mosquitto.git

WORKDIR /build/mosquitto
RUN cmake -B build -DCMAKE_BUILD_TYPE=Release \
        -DWITH_WEBSOCKETS=ON -DWITH_DOCS=OFF -DWITH_TESTS=OFF -DWITH_STATIC_LIBRARIES=OFF \
        -DCMAKE_INSTALL_PREFIX=/opt/mosquitto && \
    cmake --build build -j"$(nproc)" && \
    cmake --install build

##############################################
# Stage 2: build the TrustMQTT plugin against those broker headers
##############################################
FROM alpine:3.19 AS plugin-builder

# cjson-dev is required because Mosquitto 2.1.0's public headers (mosquitto.h
# -> libcommon.h -> libcommon_cjson.h) transitively #include <cjson/cJSON.h>,
# so anything compiling against the broker headers needs cJSON's headers too.
RUN apk add --no-cache build-base cmake pkgconfig hiredis-dev openssl-dev cjson-dev

WORKDIR /build/plugin
COPY --from=mosquitto-builder /opt/mosquitto/include/ /opt/mosquitto/include/
COPY plugin/include/ include/
COPY plugin/src/ src/
COPY plugin/CMakeLists.txt .

RUN mkdir -p build && cd build && \
    cmake .. -DMOSQUITTO_BROKER_INCLUDE_DIR=/opt/mosquitto/include && \
    cmake --build . --config Release

##############################################
# Stage 3: runtime
##############################################
FROM alpine:3.19

RUN apk add --no-cache openssl libwebsockets c-ares hiredis libstdc++ cjson \
        sqlite-libs argon2-libs \
    && addgroup -S mosquitto && adduser -S -G mosquitto -H mosquitto

COPY --from=mosquitto-builder /opt/mosquitto/sbin/mosquitto /usr/local/sbin/mosquitto
COPY --from=mosquitto-builder /opt/mosquitto/lib/ /usr/local/lib/
COPY --from=plugin-builder /build/plugin/build/trustmqtt_plugin.so /mosquitto/plugin/trustmqtt_plugin.so

RUN mkdir -p /mosquitto/config /mosquitto/data /mosquitto/log && \
    chown -R mosquitto:mosquitto /mosquitto

COPY docker/mosquitto/mosquitto.conf /mosquitto/config/mosquitto.conf
COPY docker/mosquitto/acl.conf /mosquitto/config/acl.conf
# Mosquitto 2.1.x warns (and future versions will refuse) if the ACL file is
# world-readable or not owned by the broker user; lock it down at build time.
RUN chown mosquitto:mosquitto /mosquitto/config/acl.conf && \
    chmod 0600 /mosquitto/config/acl.conf

ENV LD_LIBRARY_PATH=/usr/local/lib
EXPOSE 1883 9001

USER mosquitto
ENTRYPOINT ["/usr/local/sbin/mosquitto"]
CMD ["-c", "/mosquitto/config/mosquitto.conf"]
