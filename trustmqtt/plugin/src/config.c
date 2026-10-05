#include "trustmqtt_core.h"

#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

static void copy_string(char *dst, size_t dst_size, const char *src)
{
    if (!dst || dst_size == 0) {
        return;
    }
    if (!src) {
        dst[0] = '\0';
        return;
    }
    strncpy(dst, src, dst_size - 1);
    dst[dst_size - 1] = '\0';
}

static int parse_positive_int(const char *value, int *out)
{
    if (!value || !*value) {
        return 0;
    }
    errno = 0;
    char *end = NULL;
    long parsed = strtol(value, &end, 10);
    if (errno || !end || *end != '\0' || parsed <= 0 || parsed > INT_MAX) {
        return 0;
    }
    *out = (int)parsed;
    return 1;
}

void tmq_config_defaults(tmq_config_t *config, const char *adapter_name)
{
    if (!config) {
        return;
    }
    memset(config, 0, sizeof(*config));
    copy_string(config->broker, sizeof(config->broker),
                adapter_name && *adapter_name ? adapter_name : "unknown");
    copy_string(config->broker_id, sizeof(config->broker_id), "default");
    copy_string(config->redis_host, sizeof(config->redis_host), "redis");
    config->redis_port = 6379;
    config->emit_batch_ms = 100;
    config->verdict_refresh_ms = 500;
    config->mode = TMQ_MODE_ENFORCE;
    config->payload_hash_enabled = 0;
}

int tmq_config_set(tmq_config_t *config, const char *key, const char *value)
{
    if (!config || !key || !value) {
        return -1;
    }
    if (strcmp(key, "broker_id") == 0) {
        if (!*value) return -1;
        copy_string(config->broker_id, sizeof(config->broker_id), value);
    } else if (strcmp(key, "redis_host") == 0) {
        if (!*value) return -1;
        copy_string(config->redis_host, sizeof(config->redis_host), value);
    } else if (strcmp(key, "redis_port") == 0) {
        if (!parse_positive_int(value, &config->redis_port) || config->redis_port > 65535) return -1;
    } else if (strcmp(key, "emit_batch_ms") == 0) {
        if (!parse_positive_int(value, &config->emit_batch_ms)) return -1;
    } else if (strcmp(key, "verdict_refresh_ms") == 0) {
        if (!parse_positive_int(value, &config->verdict_refresh_ms)) return -1;
    } else if (strcmp(key, "mode") == 0) {
        if (strcmp(value, "enforce") == 0) {
            config->mode = TMQ_MODE_ENFORCE;
        } else if (strcmp(value, "monitor") == 0) {
            config->mode = TMQ_MODE_MONITOR;
        } else if (strcmp(value, "fingerprint") == 0) {
            config->mode = TMQ_MODE_FINGERPRINT;
        } else {
            return -1;
        }
    } else if (strcmp(key, "payload_hash") == 0) {
        if (strcmp(value, "sha256") == 0) {
            config->payload_hash_enabled = 1;
        } else if (strcmp(value, "off") == 0) {
            config->payload_hash_enabled = 0;
        } else {
            return -1;
        }
    } else {
        return 0;
    }
    return 1;
}
