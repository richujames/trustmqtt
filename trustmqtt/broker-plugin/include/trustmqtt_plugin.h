#ifndef TRUSTMQTT_PLUGIN_H
#define TRUSTMQTT_PLUGIN_H

#include <stdint.h>

typedef struct device_metadata_t {
    char client_id[256];
    char event_type[32];
    uint64_t timestamp_ms;

    /* connect */
    int protocol_version;
    int clean_session;
    int keep_alive_sec;
    int will_flag;
    char will_topic[256];
    int will_qos;
    uint32_t session_expiry_interval;

    /* publish */
    char topic[512];
    int qos;
    int retain;
    int dup;
    int payload_size_bytes;
    int packet_id;

    /* disconnect */
    int reason_code;
    int clean;
} device_metadata_t;

char *serialize_metadata_json(const device_metadata_t *m);
void free_serialized(char *s);

/* Redis worker API (background thread, non-blocking from plugin hooks) */
int redis_worker_init(const char *host, int port);
void redis_worker_cleanup(void);
int enqueue_metadata_json(const char *json);

#endif
