#include <string.h>
#include <time.h>
#include "trustmqtt_plugin.h"

#ifdef HAVE_MOSQUITTO
#include <mosquitto.h>
#include <mosquitto_broker.h>
#include <mosquitto_plugin.h>
#endif

/* SUBSCRIBE hook using MOSQ_EVT_ACL_CHECK */
int handle_subscribe_event(void *userdata, const void *event)
{
    (void)userdata;
    if (!event) return 0;

#ifdef HAVE_MOSQUITTO
    const struct mosquitto_evt_acl_check *evt = (const struct mosquitto_evt_acl_check *)event;
    /* We use the ACL check event to intercept subscriptions.
     * MOSQ_ACL_SUBSCRIBE indicates this is a subscription attempt. */
    if (evt->access != MOSQ_ACL_SUBSCRIBE) {
        return 0; 
    }
#endif

    device_metadata_t m;
    memset(&m, 0, sizeof(m));
    strncpy(m.event_type, "SUBSCRIBE", sizeof(m.event_type)-1);
    m.timestamp_ms = (uint64_t)time(NULL) * 1000ULL;

#ifdef HAVE_MOSQUITTO
    if (evt->client) {
        const char *id = mosquitto_client_id(evt->client);
        if (id) {
            strncpy(m.client_id, id, sizeof(m.client_id)-1);
        } else {
            strncpy(m.client_id, "unknown", sizeof(m.client_id)-1);
        }
    } else {
        strncpy(m.client_id, "unknown", sizeof(m.client_id)-1);
    }
    
    /* In an ACL check for subscribe, evt->topic holds the subscription filter */
    if (evt->topic) {
        strncpy(m.topic, evt->topic, sizeof(m.topic)-1);
    }
    m.qos = evt->qos;
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
