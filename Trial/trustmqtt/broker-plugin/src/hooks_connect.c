#include <string.h>
#include <time.h>
#include "trustmqtt_plugin.h"

/* Minimal CONNECT hook: populate basic fields and enqueue JSON. Integration
 * with Mosquitto event structs should replace the placeholder extraction
 * when wiring into a real build environment. */
int handle_connect_event(void *userdata, const void *event)
{
    (void)userdata; (void)event;
    device_metadata_t m;
    memset(&m, 0, sizeof(m));
    strncpy(m.event_type, "CONNECT", sizeof(m.event_type)-1);
    /* TODO: extract client_id, protocol_version, clean_session, keep_alive from event */
    strncpy(m.client_id, "unknown", sizeof(m.client_id)-1);
    m.timestamp_ms = (uint64_t)time(NULL) * 1000ULL;

    char *json = serialize_metadata_json(&m);
    if (json) {
        enqueue_metadata_json(json);
        free_serialized(json);
    }
    return 0;
}
