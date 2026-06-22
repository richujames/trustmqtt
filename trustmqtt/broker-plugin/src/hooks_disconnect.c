#include <string.h>
#include <time.h>
#include "trustmqtt_plugin.h"

#ifdef HAVE_MOSQUITTO
#include <mosquitto.h>
#include <mosquitto_broker.h>
#include <mosquitto_plugin.h>
#endif

/* DISCONNECT hook using MOSQ_EVT_DISCONNECT */
int handle_disconnect_event(void *userdata, const void *event)
{
    (void)userdata;
    if (!event) return 0;

    device_metadata_t m;
    memset(&m, 0, sizeof(m));
    strncpy(m.event_type, "DISCONNECT", sizeof(m.event_type)-1);
    m.timestamp_ms = (uint64_t)time(NULL) * 1000ULL;

#ifdef HAVE_MOSQUITTO
    const struct mosquitto_evt_disconnect *evt = (const struct mosquitto_evt_disconnect *)event;
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
    
    m.reason_code = evt->reason;
    /* A reason code of 0 generally signifies a clean, intentional disconnect in MQTT. */
    m.clean = (evt->reason == 0) ? 1 : 0;
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
