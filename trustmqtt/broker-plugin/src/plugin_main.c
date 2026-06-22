/* plugin_main.c — Mosquitto v2 plugin API (plugin version 5)
 *
 * Uses the correct Mosquitto 2.x callback registration API:
 *   mosquitto_callback_register(identifier, event, callback, event_data, userdata)
 *
 * Mosquitto 2.x supports these plugin events:
 *   MOSQ_EVT_RELOAD, MOSQ_EVT_ACL_CHECK, MOSQ_EVT_BASIC_AUTH,
 *   MOSQ_EVT_EXT_AUTH_START, MOSQ_EVT_EXT_AUTH_CONTINUE,
 *   MOSQ_EVT_CONTROL, MOSQ_EVT_MESSAGE, MOSQ_EVT_PSK_KEY,
 *   MOSQ_EVT_TICK, MOSQ_EVT_DISCONNECT
 *
 * Note: there is no MOSQ_EVT_CONNECT or MOSQ_EVT_SUBSCRIBE in Mosquitto 2.x.
 * We use MOSQ_EVT_BASIC_AUTH to capture connect events (fires on every auth attempt),
 * MOSQ_EVT_MESSAGE to capture publishes, MOSQ_EVT_ACL_CHECK for subscribe detection,
 * and MOSQ_EVT_DISCONNECT for disconnects.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "trustmqtt_plugin.h"

#ifdef HAVE_MOSQUITTO
#include <mosquitto.h>
#include <mosquitto_broker.h>
#include <mosquitto_plugin.h>

/* Global plugin identifier — required by mosquitto_callback_register */
static mosquitto_plugin_id_t *mosq_plugin_id = NULL;
#else
/* Fallback definitions when building without Mosquitto headers */
#ifndef MOSQ_ERR_SUCCESS
#define MOSQ_ERR_SUCCESS 0
#endif
struct mosquitto_opt { char *key; char *value; };
typedef void mosquitto_plugin_id_t;
#endif

/* Forward declarations of hook handlers (defined in hooks_*.c) */
int handle_connect_event(void *userdata, const void *event);
int handle_publish_event(void *userdata, const void *event);
int handle_subscribe_event(void *userdata, const void *event);
int handle_disconnect_event(void *userdata, const void *event);

/*
 * mosquitto_plugin_version — tell Mosquitto which plugin API versions we support.
 * Mosquitto 2.x expects this to fill an array with supported versions.
 */
int mosquitto_plugin_version(int supported_version_count, const int *supported_versions)
{
    (void)supported_version_count;
    (void)supported_versions;
    return 5; /* Plugin API v5 */
}

/*
 * mosquitto_plugin_init — called once when the plugin is loaded.
 * Correct signature for Mosquitto 2.x plugin API v5.
 */
int mosquitto_plugin_init(mosquitto_plugin_id_t *identifier,
                          void **user_data,
                          struct mosquitto_opt *options,
                          int option_count)
{
    (void)user_data;
    (void)options;
    (void)option_count;

    printf("trustmqtt_plugin: init\n");

#ifdef HAVE_MOSQUITTO
    mosq_plugin_id = identifier;

    /* Initialize background Redis worker.
     * Default host/port; can be made configurable via plugin options. */
    const char *redis_host = "redis";
    int redis_port = 6379;

    /* Check plugin options for redis_host / redis_port overrides */
    for (int i = 0; i < option_count; i++) {
        if (strcmp(options[i].key, "redis_host") == 0) {
            redis_host = options[i].value;
        } else if (strcmp(options[i].key, "redis_port") == 0) {
            redis_port = atoi(options[i].value);
        }
    }

    if (redis_worker_init(redis_host, redis_port) != 0) {
        fprintf(stderr, "trustmqtt_plugin: failed to start redis worker\n");
    }

    /* Register callbacks with the broker.
     * Signature: mosquitto_callback_register(id, event, callback, event_data, userdata)
     */

    /* MOSQ_EVT_BASIC_AUTH fires on every client connection (auth attempt).
     * We use this as our "connect" event to fingerprint the client. */
    mosquitto_callback_register(mosq_plugin_id,
                                MOSQ_EVT_BASIC_AUTH,
                                (MOSQ_FUNC_generic_callback)handle_connect_event,
                                NULL, NULL);

    /* MOSQ_EVT_MESSAGE fires on every PUBLISH received by the broker. */
    mosquitto_callback_register(mosq_plugin_id,
                                MOSQ_EVT_MESSAGE,
                                (MOSQ_FUNC_generic_callback)handle_publish_event,
                                NULL, NULL);

    /* MOSQ_EVT_ACL_CHECK fires on subscribe/publish ACL checks.
     * We filter for subscribe operations inside the handler. */
    mosquitto_callback_register(mosq_plugin_id,
                                MOSQ_EVT_ACL_CHECK,
                                (MOSQ_FUNC_generic_callback)handle_subscribe_event,
                                NULL, NULL);

    /* MOSQ_EVT_DISCONNECT fires when a client disconnects. */
    mosquitto_callback_register(mosq_plugin_id,
                                MOSQ_EVT_DISCONNECT,
                                (MOSQ_FUNC_generic_callback)handle_disconnect_event,
                                NULL, NULL);
#else
    (void)identifier;
    /* Mosquitto headers not found; hooks are available as stubs in source.
     * Still init the redis worker for testing purposes. */
    if (redis_worker_init("127.0.0.1", 6379) != 0) {
        fprintf(stderr, "trustmqtt_plugin: failed to start redis worker\n");
    }
#endif

    return MOSQ_ERR_SUCCESS;
}

/*
 * mosquitto_plugin_cleanup — called when the plugin is unloaded.
 */
int mosquitto_plugin_cleanup(void *user_data,
                             struct mosquitto_opt *options,
                             int option_count)
{
    (void)user_data;
    (void)options;
    (void)option_count;

    printf("trustmqtt_plugin: cleanup\n");

#ifdef HAVE_MOSQUITTO
    /* Unregister all callbacks */
    if (mosq_plugin_id) {
        mosquitto_callback_unregister(mosq_plugin_id,
                                      MOSQ_EVT_BASIC_AUTH,
                                      (MOSQ_FUNC_generic_callback)handle_connect_event,
                                      NULL);
        mosquitto_callback_unregister(mosq_plugin_id,
                                      MOSQ_EVT_MESSAGE,
                                      (MOSQ_FUNC_generic_callback)handle_publish_event,
                                      NULL);
        mosquitto_callback_unregister(mosq_plugin_id,
                                      MOSQ_EVT_ACL_CHECK,
                                      (MOSQ_FUNC_generic_callback)handle_subscribe_event,
                                      NULL);
        mosquitto_callback_unregister(mosq_plugin_id,
                                      MOSQ_EVT_DISCONNECT,
                                      (MOSQ_FUNC_generic_callback)handle_disconnect_event,
                                      NULL);
    }
#endif

    redis_worker_cleanup();
    return MOSQ_ERR_SUCCESS;
}
