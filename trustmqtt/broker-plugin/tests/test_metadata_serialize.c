#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include "trustmqtt_plugin.h"

int main(void)
{
    device_metadata_t m;
    memset(&m, 0, sizeof(m));
    strncpy(m.event_type, "TEST", sizeof(m.event_type) - 1);
    strncpy(m.client_id, "client123", sizeof(m.client_id) - 1);
    m.timestamp_ms = 1234567890ULL;

    char *json = serialize_metadata_json(&m);
    assert(json != NULL);
    assert(strstr(json, "\"event_type\":\"TEST\"") != NULL);
    assert(strstr(json, "\"client_id\":\"client123\"") != NULL);

    free_serialized(json);
    return 0;
}
