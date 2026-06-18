#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "trustmqtt_plugin.h"

char *serialize_metadata_json(const device_metadata_t *m)
{
    /* Very small hand-rolled serializer for v1 schema. Caller frees the result. */
    char *buf = malloc(4096);
    if(!buf) return NULL;
    snprintf(buf, 4096,
        "{\"schema_version\":\"1.0\",\"event_type\":\"%s\",\"client_id\":\"%s\",\"timestamp_ms\":%llu}",
        m->event_type, m->client_id, (unsigned long long)m->timestamp_ms);
    return buf;
}

void free_serialized(char *s){ free(s); }
