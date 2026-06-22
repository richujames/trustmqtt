##############################################
# Stage 1: Build the C plugin (Alpine-based to match runtime)
##############################################
FROM alpine:3.19 AS builder

RUN apk add --no-cache \
        build-base cmake pkgconfig \
        hiredis-dev \
        mosquitto-dev

WORKDIR /build/broker-plugin

# Copy plugin source
COPY include/ include/
COPY src/ src/
COPY CMakeLists.txt .

# Build the shared library
RUN mkdir -p build && cd build && \
    cmake .. && \
    cmake --build . --config Release

##############################################
# Stage 2: Runtime — Eclipse Mosquitto (Alpine)
##############################################
FROM eclipse-mosquitto:2.0

# Copy the compiled plugin AND the exact hiredis .so it was linked against
# from the builder — avoids SONAME mismatch with the runtime's Alpine version.
USER root
COPY --from=builder /build/broker-plugin/build/trustmqtt_plugin.so /mosquitto/plugin/trustmqtt_plugin.so
COPY --from=builder /usr/lib/libhiredis.so* /usr/lib/

USER mosquitto
