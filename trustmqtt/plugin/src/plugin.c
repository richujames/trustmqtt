/* plugin.c — Mosquitto 2.1.x adapter for the broker-neutral TrustMQTT core.
 *
 * This file contains only Mosquitto API translation and lifecycle mapping.
 * Event serialization, Redis I/O, verdict refresh, and policy enforcement
 * live in core.c and are shared by every broker adapter.
 */
#include "trustmqtt_core.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef HAVE_MOSQUITTO
#include <mosquitto.h>
#else
/* Degraded build for local syntax checks without broker headers. */
struct mosquitto_opt { char *key; char *value; };
typedef void mosquitto_plugin_id_t;
#ifndef MOSQ_ERR_SUCCESS
#define MOSQ_ERR_SUCCESS 0
#endif
#ifndef MOSQ_ERR_ACL_DENIED
#define MOSQ_ERR_ACL_DENIED 12
#endif
#ifndef MOSQ_ERR_PLUGIN_DEFER
#define MOSQ_ERR_PLUGIN_DEFER (-2)
#endif
#ifndef MOSQ_ERR_NOMEM
#define MOSQ_ERR_NOMEM 1
#endif
#ifndef MOSQ_ERR_INVAL
#define MOSQ_ERR_INVAL 3
#endif
#endif

typedef struct {
    tmq_core_t *core;
#ifdef HAVE_MOSQUITTO
    mosquitto_plugin_id_t *plugin_id;
#endif
} tmq_mosquitto_adapter_t;

static tmq_mosquitto_adapter_t g_adapter;

#ifdef HAVE_MOSQUITTO
static const char *protocol_name(int protocol)
{
    switch (protocol) {
        case mp_mqtt: return "mqtt";
        case mp_mqttsn: return "mqttsn";
        case mp_websockets: return "websockets";
        case mp_http_api: return "http_api";
        default: return "unknown";
    }
}

static int count_user_properties(const mosquitto_property *properties)
{
    int count = 0;
    bool skip_first = false;
    const mosquitto_property *cursor = properties;
    while (cursor) {
        char *name = NULL;
        char *value = NULL;
        cursor = mosquitto_property_read_string_pair(cursor, MQTT_PROP_USER_PROPERTY,
                                                     &name, &value, skip_first);
        if (!cursor) break;
        count++;
        free(name);
        free(value);
        skip_first = true;
    }
    return count;
}

static int handle_connect(int event, void *event_data, void *userdata)
{
    (void)event;
    (void)userdata;
    struct mosquitto_evt_connect *native = event_data;
    const char *client_id = mosquitto_client_id(native->client);
    if (!client_id) return MOSQ_ERR_SUCCESS;

    tmq_connect_event_t normalized = {
        .client_id = client_id,
        .username = mosquitto_client_username(native->client),
        .ip = mosquitto_client_address(native->client),
        .protocol = protocol_name(mosquitto_client_protocol(native->client)),
        .clean_session = mosquitto_client_clean_session(native->client),
        .has_clean_session = 1,
        .keepalive = mosquitto_client_keepalive(native->client),
    };
    tmq_core_on_connect(g_adapter.core, &normalized);
    return MOSQ_ERR_SUCCESS;
}

static int handle_disconnect(int event, void *event_data, void *userdata)
{
    (void)event;
    (void)userdata;
    struct mosquitto_evt_disconnect *native = event_data;
    const char *client_id = mosquitto_client_id(native->client);
    tmq_core_on_disconnect(g_adapter.core, client_id, native->reason, 1, 0);
    return MOSQ_ERR_SUCCESS;
}

static int handle_client_offline(int event, void *event_data, void *userdata)
{
    (void)event;
    (void)userdata;
    struct mosquitto_evt_client_offline *native = event_data;
    tmq_core_on_disconnect(g_adapter.core, mosquitto_client_id(native->client), 0, 0, 1);
    return MOSQ_ERR_SUCCESS;
}

static int handle_message_in(int event, void *event_data, void *userdata)
{
    (void)event;
    (void)userdata;
    struct mosquitto_evt_message *native = event_data;
    const char *client_id = mosquitto_client_id(native->client);
    if (!client_id) return MOSQ_ERR_SUCCESS;

    char *content_type = NULL;
    uint32_t message_expiry = 0;
    int user_prop_count = 0;
    if (native->properties) {
        mosquitto_property_read_string(native->properties, MQTT_PROP_CONTENT_TYPE,
                                       &content_type, false);
        mosquitto_property_read_int32(native->properties,
                                      MQTT_PROP_MESSAGE_EXPIRY_INTERVAL,
                                      &message_expiry, false);
        user_prop_count = count_user_properties(native->properties);
    }
    tmq_publish_event_t normalized = {
        .client_id = client_id,
        .topic = native->topic,
        .payload = native->payload,
        .payload_len = native->payloadlen,
        .qos = native->qos,
        .retain = native->retain,
        .content_type = content_type,
        .message_expiry = message_expiry,
        .user_prop_count = user_prop_count,
    };
    tmq_core_on_publish(g_adapter.core, &normalized);
    free(content_type);
    return MOSQ_ERR_SUCCESS;
}

static int handle_subscribe(int event, void *event_data, void *userdata)
{
    (void)event;
    (void)userdata;
    struct mosquitto_evt_subscribe *native = event_data;
    const char *client_id = mosquitto_client_id(native->client);
    if (client_id) {
        tmq_core_on_subscribe(g_adapter.core, client_id, native->data.topic_filter,
                              native->data.options & 0x03,
                              mosquitto_client_sub_count(native->client));
    }
    return MOSQ_ERR_SUCCESS;
}

static int handle_unsubscribe(int event, void *event_data, void *userdata)
{
    (void)event;
    (void)userdata;
    struct mosquitto_evt_unsubscribe *native = event_data;
    const char *client_id = mosquitto_client_id(native->client);
    if (client_id) {
        tmq_core_on_unsubscribe(g_adapter.core, client_id, native->data.topic_filter,
                                native->data.options & 0x03,
                                mosquitto_client_sub_count(native->client));
    }
    return MOSQ_ERR_SUCCESS;
}

static int handle_basic_auth(int event, void *event_data, void *userdata)
{
    (void)event;
    (void)userdata;
    struct mosquitto_evt_basic_auth *native = event_data;
    tmq_core_on_auth_observe(g_adapter.core,
                             mosquitto_client_id(native->client),
                             native->username,
                             mosquitto_client_address(native->client));
    return MOSQ_ERR_PLUGIN_DEFER;
}

static int handle_acl_check(int event, void *event_data, void *userdata)
{
    (void)event;
    (void)userdata;
    struct mosquitto_evt_acl_check *native = event_data;
    const char *client_id = mosquitto_client_id(native->client);
    if (!client_id) return MOSQ_ERR_PLUGIN_DEFER;

    tmq_access_t access;
    if (native->access & MOSQ_ACL_SUBSCRIBE) {
        access = TMQ_ACCESS_SUBSCRIBE;
    } else if (native->access & MOSQ_ACL_WRITE) {
        access = TMQ_ACCESS_WRITE;
    } else {
        access = TMQ_ACCESS_READ;
    }
    return tmq_core_check_access(g_adapter.core, client_id, native->topic, access) == TMQ_ENFORCE_DENY
               ? MOSQ_ERR_ACL_DENIED
               : MOSQ_ERR_PLUGIN_DEFER;
}

static int handle_tick(int event, void *event_data, void *userdata)
{
    (void)event;
    (void)event_data;
    (void)userdata;
    /* Redis maintenance stays on the core thread. Broker API calls, such as
     * kicking a client, are claimed here on Mosquitto's own event thread. */
    tmq_core_poll_actions(g_adapter.core);
    return MOSQ_ERR_SUCCESS;
}

static void kick_client(const char *client_id, void *userdata)
{
    (void)userdata;
    mosquitto_kick_client_by_clientid(client_id, false);
}

static void unregister_callbacks(void)
{
    mosquitto_plugin_id_t *id = g_adapter.plugin_id;
    if (!id) return;
    mosquitto_callback_unregister(id, MOSQ_EVT_CONNECT,
                                  (MOSQ_FUNC_generic_callback)handle_connect, NULL);
    mosquitto_callback_unregister(id, MOSQ_EVT_DISCONNECT,
                                  (MOSQ_FUNC_generic_callback)handle_disconnect, NULL);
    mosquitto_callback_unregister(id, MOSQ_EVT_CLIENT_OFFLINE,
                                  (MOSQ_FUNC_generic_callback)handle_client_offline, NULL);
    mosquitto_callback_unregister(id, MOSQ_EVT_MESSAGE_IN,
                                  (MOSQ_FUNC_generic_callback)handle_message_in, NULL);
    mosquitto_callback_unregister(id, MOSQ_EVT_SUBSCRIBE,
                                  (MOSQ_FUNC_generic_callback)handle_subscribe, NULL);
    mosquitto_callback_unregister(id, MOSQ_EVT_UNSUBSCRIBE,
                                  (MOSQ_FUNC_generic_callback)handle_unsubscribe, NULL);
    mosquitto_callback_unregister(id, MOSQ_EVT_BASIC_AUTH,
                                  (MOSQ_FUNC_generic_callback)handle_basic_auth, NULL);
    mosquitto_callback_unregister(id, MOSQ_EVT_ACL_CHECK,
                                  (MOSQ_FUNC_generic_callback)handle_acl_check, NULL);
    mosquitto_callback_unregister(id, MOSQ_EVT_TICK,
                                  (MOSQ_FUNC_generic_callback)handle_tick, NULL);
}
#else
static void kick_client(const char *client_id, void *userdata)
{
    (void)client_id;
    (void)userdata;
}
#endif /* HAVE_MOSQUITTO */

int mosquitto_plugin_version(int supported_version_count, const int *supported_versions)
{
    (void)supported_version_count;
    (void)supported_versions;
    return 5;
}

int mosquitto_plugin_init(mosquitto_plugin_id_t *identifier, void **user_data,
                          struct mosquitto_opt *options, int option_count)
{
    (void)user_data;
    memset(&g_adapter, 0, sizeof(g_adapter));
    tmq_config_t config;
    tmq_config_defaults(&config, "mosquitto");
    for (int i = 0; i < option_count; i++) {
        if (!options[i].key || !options[i].value) continue;
        int result = tmq_config_set(&config, options[i].key, options[i].value);
        if (result < 0) {
            fprintf(stderr, "trustmqtt_mosquitto: invalid option %s=%s\n",
                    options[i].key, options[i].value);
            return MOSQ_ERR_INVAL;
        }
    }
    g_adapter.core = tmq_core_create(&config, kick_client, NULL);
    if (!g_adapter.core) {
        fprintf(stderr, "trustmqtt_mosquitto: core initialization failed\n");
        return MOSQ_ERR_NOMEM;
    }

#ifdef HAVE_MOSQUITTO
    g_adapter.plugin_id = identifier;
    mosquitto_callback_register(identifier, MOSQ_EVT_CONNECT,
                                (MOSQ_FUNC_generic_callback)handle_connect, NULL, NULL);
    mosquitto_callback_register(identifier, MOSQ_EVT_DISCONNECT,
                                (MOSQ_FUNC_generic_callback)handle_disconnect, NULL, NULL);
    mosquitto_callback_register(identifier, MOSQ_EVT_CLIENT_OFFLINE,
                                (MOSQ_FUNC_generic_callback)handle_client_offline, NULL, NULL);
    mosquitto_callback_register(identifier, MOSQ_EVT_MESSAGE_IN,
                                (MOSQ_FUNC_generic_callback)handle_message_in, NULL, NULL);
    mosquitto_callback_register(identifier, MOSQ_EVT_SUBSCRIBE,
                                (MOSQ_FUNC_generic_callback)handle_subscribe, NULL, NULL);
    mosquitto_callback_register(identifier, MOSQ_EVT_UNSUBSCRIBE,
                                (MOSQ_FUNC_generic_callback)handle_unsubscribe, NULL, NULL);
    mosquitto_callback_register(identifier, MOSQ_EVT_BASIC_AUTH,
                                (MOSQ_FUNC_generic_callback)handle_basic_auth, NULL, NULL);
    mosquitto_callback_register(identifier, MOSQ_EVT_ACL_CHECK,
                                (MOSQ_FUNC_generic_callback)handle_acl_check, NULL, NULL);
    mosquitto_callback_register(identifier, MOSQ_EVT_TICK,
                                (MOSQ_FUNC_generic_callback)handle_tick, NULL, NULL);
#else
    (void)identifier;
#endif
    printf("trustmqtt_mosquitto: init (mode=%d, broker_id=%s, redis=%s:%d)\n",
           config.mode, config.broker_id, config.redis_host, config.redis_port);
    return MOSQ_ERR_SUCCESS;
}

int mosquitto_plugin_cleanup(void *user_data, struct mosquitto_opt *options,
                             int option_count)
{
    (void)user_data;
    (void)options;
    (void)option_count;
#ifdef HAVE_MOSQUITTO
    unregister_callbacks();
#endif
    tmq_core_destroy(g_adapter.core);
    memset(&g_adapter, 0, sizeof(g_adapter));
    return MOSQ_ERR_SUCCESS;
}
