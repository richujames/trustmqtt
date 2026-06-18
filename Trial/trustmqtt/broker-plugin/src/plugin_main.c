/* Minimal plugin_main stub - expand when implementing real hooks */
#include <stdio.h>
#include "trustmqtt_plugin.h"
#include <stdlib.h>
#include <string.h>

/* Forward declarations of hook handlers */
int handle_connect_event(void *userdata, const void *event);
int handle_publish_event(void *userdata, const void *event);
int handle_subscribe_event(void *userdata, const void *event);
int handle_disconnect_event(void *userdata, const void *event);

#ifdef HAVE_MOSQUITTO
#include <mosquitto_broker.h>
#include <mosquitto_plugin.h>
#endif

int mosquitto_plugin_version(void)
{
    return 5; /* MOSQUITTO plugin API v5 */
}

int mosquitto_plugin_init(void **user_data, void *options, int option_count)
{
    (void)user_data; (void)options; (void)option_count;
    printf("trustmqtt_plugin: init\n");
    /* Initialize background Redis worker (host/port can be made configurable) */
    if (redis_worker_init("127.0.0.1", 6379) != 0) {
        fprintf(stderr, "trustmqtt_plugin: failed to start redis worker\n");
    }
#ifdef HAVE_MOSQUITTO
    /* Register callbacks with Mosquitto broker if headers are available.
     * The exact event constants and callback signatures are provided by
     * the Mosquitto broker API. Adjust as needed for the target Mosquitto version.
     */
    mosquitto_callback_register(MOSQ_EVT_CONNECT, handle_connect_event, NULL);
    mosquitto_callback_register(MOSQ_EVT_MESSAGE, handle_publish_event, NULL);
    mosquitto_callback_register(MOSQ_EVT_SUBSCRIBE, handle_subscribe_event, NULL);
    mosquitto_callback_register(MOSQ_EVT_DISCONNECT, handle_disconnect_event, NULL);
#else
    /* Mosquitto headers not found; hooks are available as stubs in source. */
#endif
    return 0;
}

int mosquitto_plugin_cleanup(void *user_data, void *options, int option_count)
{
    (void)user_data; (void)options; (void)option_count;
    printf("trustmqtt_plugin: cleanup\n");
    redis_worker_cleanup();
    return 0;
}
