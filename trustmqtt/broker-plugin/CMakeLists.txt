cmake_minimum_required(VERSION 3.5)
project(trustmqtt_plugin C)

set(CMAKE_C_STANDARD 11)
find_package(PkgConfig REQUIRED)
pkg_check_modules(HIREDIS hiredis)

include_directories(${HIREDIS_INCLUDE_DIRS} include)
link_directories(${HIREDIS_LIBRARY_DIRS})

# Optional: enable Mosquitto broker headers integration if available
option(USE_MOSQUITTO "Enable mosquitto broker integration if headers available" ON)
if(USE_MOSQUITTO)
  find_path(MOSQUITTO_BROKER_INCLUDE_DIR mosquitto_broker.h)
  if(MOSQUITTO_BROKER_INCLUDE_DIR)
    message(STATUS "Found mosquitto broker headers in ${MOSQUITTO_BROKER_INCLUDE_DIR}")
    include_directories(${MOSQUITTO_BROKER_INCLUDE_DIR})
    add_definitions(-DHAVE_MOSQUITTO)
  else()
    message(STATUS "Mosquitto broker headers not found; building without broker callback registration")
  endif()
endif()

add_library(trustmqtt_plugin SHARED
  src/plugin_main.c
  src/hooks_connect.c
  src/hooks_publish.c
  src/hooks_subscribe.c
  src/hooks_disconnect.c
  src/metadata_serialize.c
  src/redis_publish.c
)

target_link_libraries(trustmqtt_plugin ${HIREDIS_LIBRARIES})

find_package(Threads REQUIRED)
if(Threads_FOUND)
  target_link_libraries(trustmqtt_plugin ${CMAKE_THREAD_LIBS_INIT})
endif()

set_target_properties(trustmqtt_plugin PROPERTIES
  PREFIX ""
  OUTPUT_NAME "trustmqtt_plugin"
)

install(TARGETS trustmqtt_plugin DESTINATION build)
