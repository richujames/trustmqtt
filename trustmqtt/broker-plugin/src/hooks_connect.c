#include <string.h>
#include <time.h>
#include "trustmqtt_plugin.h"

#ifdef HAVE_MOSQUITTO
#include <mosquitto.h>
#include <mosquitto_broker.h>
#include <mosquitto_plugin.h>
#endif

/* CONNECT hook using MOSQ_EVT_BASIC_AUTH: fires on connection auth attempt */
int handle_connect_event(void *userdata, const void *event)
{
    (void)userdata;
    if (!event) return 0;

    device_metadata_t m;
    memset(&m, 0, sizeof(m));
    strncpy(m.event_type, "CONNECT", sizeof(m.event_type)-1);
    m.timestamp_ms = (uint64_t)time(NULL) * 1000ULL;

#ifdef HAVE_MOSQUITTO
    const struct mosquitto_evt_basic_auth *evt = (const struct mosquitto_evt_basic_auth *)event;
    if (evt->client) {
        const char *id = mosquitto_client_id(evt->client);
        if (id) {
            strncpy(m.client_id, id, sizeof(m.client_id)-1);
        } else {
            strncpy(m.client_id, "unknown", sizeof(m.client_id)-1);
        }
        m.protocol_version = mosquitto_client_protocol_version(evt->client);
        m.clean_session = mosquitto_client_clean_session(evt->client);
        m.keep_alive_sec = mosquitto_client_keepalive(evt->client);
        /* Will flag and session expiry are not easily accessible from the basic auth event,
         * so we leave them at default/0 for v1. */
    } else {
        strncpy(m.client_id, "unknown", sizeof(m.client_id)-1);
    }
#else
    strncpy(m.client_id, "unknown", sizeof(m.client_id)-1);
#endif

    char *json = serialize_metadata_json(&m);
    if (json) {
        enqueue_metadata_json(json);
        free_serialized(json);
    }
    return 0;
}
