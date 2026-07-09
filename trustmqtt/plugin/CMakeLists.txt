cmake_minimum_required(VERSION 3.10)
project(trustmqtt_plugin C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

find_package(PkgConfig REQUIRED)
pkg_check_modules(HIREDIS REQUIRED hiredis)
# We link the same cJSON the broker uses (see plugin/src/plugin.c for why).
pkg_check_modules(CJSON REQUIRED libcjson)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

include_directories(${HIREDIS_INCLUDE_DIRS} ${CJSON_INCLUDE_DIRS} include)
link_directories(${HIREDIS_LIBRARY_DIRS} ${CJSON_LIBRARY_DIRS})

# MOSQUITTO_BROKER_INCLUDE_DIR is passed explicitly by docker/mosquitto/Dockerfile
# once it has built Mosquitto 2.1.x from source (see docs/SPEC.md §3.1). If it
# isn't supplied we still try find_path so a manual `cmake ..` against a
# system install works too; if headers truly aren't available the plugin
# builds in a degraded (event-capture-disabled) mode for local syntax checks.
if(NOT MOSQUITTO_BROKER_INCLUDE_DIR)
  find_path(MOSQUITTO_BROKER_INCLUDE_DIR mosquitto_broker.h)
endif()

if(MOSQUITTO_BROKER_INCLUDE_DIR)
  message(STATUS "Using Mosquitto broker headers from ${MOSQUITTO_BROKER_INCLUDE_DIR}")
  include_directories(${MOSQUITTO_BROKER_INCLUDE_DIR})
  add_definitions(-DHAVE_MOSQUITTO)
else()
  message(WARNING "Mosquitto broker headers not found; building in degraded mode (no event capture)")
endif()

add_library(trustmqtt_plugin SHARED
  src/plugin.c
  src/ring.c
  src/emitter.c
  src/verdict_cache.c
  src/enforce.c
)

target_link_libraries(trustmqtt_plugin
  ${HIREDIS_LIBRARIES}
  ${CJSON_LIBRARIES}
  OpenSSL::Crypto
  Threads::Threads
)

set_target_properties(trustmqtt_plugin PROPERTIES
  PREFIX ""
  OUTPUT_NAME "trustmqtt_plugin"
)

install(TARGETS trustmqtt_plugin DESTINATION build)
