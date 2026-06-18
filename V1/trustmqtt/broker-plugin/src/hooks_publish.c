#include <string.h>
#include <time.h>
#include "trustmqtt_plugin.h"

int handle_publish_event(void *userdata, const void *event)
{
    (void)userdata; (void)event;
    device_metadata_t m;
    memset(&m, 0, sizeof(m));
    strncpy(m.event_type, "PUBLISH", sizeof(m.event_type)-1);
    /* TODO: extract client_id, topic, qos, retain, dup, payload_size, packet_id */
    strncpy(m.client_id, "unknown", sizeof(m.client_id)-1);
    strncpy(m.topic, "unknown/topic", sizeof(m.topic)-1);
    m.payload_size_bytes = 0;
    m.timestamp_ms = (uint64_t)time(NULL) * 1000ULL;

    char *json = serialize_metadata_json(&m);
    if (json) {
        enqueue_metadata_json(json);
        free_serialized(json);
    }
    return 0;
}
