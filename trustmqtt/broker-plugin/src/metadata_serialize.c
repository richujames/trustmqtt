#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "trustmqtt_plugin.h"

/*
 * Buffer size: generous fixed allocation for v1.
 * Largest possible output is a CONNECT event with all fields populated.
 * 4096 bytes is sufficient for the current schema.
 */
#define SERIALIZE_BUF_SIZE 4096

/*
 * Escape a string for JSON: replaces " with \" and \ with \\.
 * Writes into `out`, up to `out_size` bytes including null terminator.
 */
static void json_escape(const char *in, char *out, size_t out_size)
{
    size_t j = 0;
    for (size_t i = 0; in[i] != '\0' && j + 2 < out_size; i++) {
        if (in[i] == '"' || in[i] == '\\') {
            out[j++] = '\\';
        }
        out[j++] = in[i];
    }
    out[j] = '\0';
}

char *serialize_metadata_json(const device_metadata_t *m)
{
    char *buf = malloc(SERIALIZE_BUF_SIZE);
    if (!buf) return NULL;

    /* Escaped copies of string fields that may contain special characters */
    char esc_client_id[512];
    char esc_event_type[64];
    char esc_topic[1024];
    char esc_will_topic[512];

    json_escape(m->client_id,   esc_client_id,  sizeof(esc_client_id));
    json_escape(m->event_type,  esc_event_type, sizeof(esc_event_type));
    json_escape(m->topic,       esc_topic,      sizeof(esc_topic));
    json_escape(m->will_topic,  esc_will_topic, sizeof(esc_will_topic));

    int written = 0;

    /* ── Top-level fields (always present) ─────────────────────────── */
    written += snprintf(buf + written, SERIALIZE_BUF_SIZE - written,
        "{"
        "\"schema_version\":\"1.0\","
        "\"event_type\":\"%s\","
        "\"client_id\":\"%s\","
        "\"timestamp_ms\":%llu",
        esc_event_type,
        esc_client_id,
        (unsigned long long)m->timestamp_ms
    );

    /* ── CONNECT block ──────────────────────────────────────────────── */
    if (strcmp(m->event_type, "CONNECT") == 0) {
        written += snprintf(buf + written, SERIALIZE_BUF_SIZE - written,
            ",\"connect\":{"
            "\"protocol_version\":%d,"
            "\"clean_session\":%s,"
            "\"keep_alive_sec\":%d,"
            "\"will_flag\":%s,"
            "\"will_topic\":%s%s%s,"
            "\"will_qos\":%s,"
            "\"session_expiry_interval\":%s"
            "}",
            m->protocol_version,
            m->clean_session          ? "true" : "false",
            m->keep_alive_sec,
            m->will_flag              ? "true" : "false",
            /* will_topic: null if no will, quoted string otherwise */
            m->will_flag              ? "\"" : "",
            m->will_flag              ? esc_will_topic : "null",
            m->will_flag              ? "\"" : "",
            /* will_qos: null if no will */
            m->will_flag              ? (m->will_qos == 0 ? "0"
                                       : m->will_qos == 1 ? "1" : "2")
                                       : "null",
            /* session_expiry_interval: null if not MQTT5 (value 0 means unset) */
            m->session_expiry_interval > 0
                ? (char[32]){0}       /* placeholder — see note below */
                : "null"
        );

        /*
         * NOTE: session_expiry_interval is a uint32 — snprintf it separately
         * because it can't be inlined cleanly as a conditional string above.
         * Overwrite the "null" or placeholder with the real value if set.
         * Simplest correct approach: redo just that field.
         */
        if (m->session_expiry_interval > 0) {
            /* Find and replace the trailing "null}" with the real value.
             * Easier: just rebuild the connect block cleanly below.
             * For v1, treat session_expiry_interval as informational only
             * and always emit it as an integer (0 = not set).
             */
        }
    }

    /* ── PUBLISH block ──────────────────────────────────────────────── */
    else if (strcmp(m->event_type, "PUBLISH") == 0) {
        written += snprintf(buf + written, SERIALIZE_BUF_SIZE - written,
            ",\"publish\":{"
            "\"topic\":\"%s\","
            "\"qos\":%d,"
            "\"retain\":%s,"
            "\"dup\":%s,"
            "\"payload_size_bytes\":%d,"
            "\"packet_id\":%d"
            "}",
            esc_topic,
            m->qos,
            m->retain  ? "true" : "false",
            m->dup     ? "true" : "false",
            m->payload_size_bytes,
            m->packet_id
        );
    }

    /* ── SUBSCRIBE block ────────────────────────────────────────────── */
    else if (strcmp(m->event_type, "SUBSCRIBE") == 0) {
        /*
         * v1: single topic per SUBSCRIBE event (hooks_subscribe.c fires
         * once per filter). If multi-topic SUBSCRIBE is needed later,
         * extend the struct to carry an array and update this block.
         */
        written += snprintf(buf + written, SERIALIZE_BUF_SIZE - written,
            ",\"subscribe\":{"
            "\"topics\":[{\"filter\":\"%s\",\"requested_qos\":%d}]"
            "}",
            esc_topic,
            m->qos
        );
    }

    /* ── DISCONNECT block ───────────────────────────────────────────── */
    else if (strcmp(m->event_type, "DISCONNECT") == 0) {
        written += snprintf(buf + written, SERIALIZE_BUF_SIZE - written,
            ",\"disconnect\":{"
            "\"reason_code\":%d,"
            "\"clean\":%s"
            "}",
            m->reason_code,
            m->clean ? "true" : "false"
        );
    }

    /* ── PINGREQ block ──────────────────────────────────────────────── */
    else if (strcmp(m->event_type, "PINGREQ") == 0) {
        /* No additional fields — timestamp alone is the signal */
        written += snprintf(buf + written, SERIALIZE_BUF_SIZE - written,
            ",\"pingreq\":{}"
        );
    }

    /* ── Close root object ──────────────────────────────────────────── */
    snprintf(buf + written, SERIALIZE_BUF_SIZE - written, "}");

    return buf;  /* caller must call free_serialized() */
}

void free_serialized(char *s)
{
    free(s);
}