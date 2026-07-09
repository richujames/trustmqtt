/* trustmqtt_plugin.h — shared types/constants for the TrustMQTT Mosquitto plugin.
 *
 * Terminology note (spec §12 rule 4): this plugin performs RESOLVED-SEMANTIC-
 * EVENT ANALYSIS (topic, QoS, retain, session state, identity, MQTT5
 * properties) — never "wire-level" or "protocol-header-level" analysis.
 * Known API limitations (not observable via the plugin hooks and therefore
 * absent from every event/feature downstream): DUP flag, packet identifier,
 * PUBACK/PUBREC/PUBREL/PUBCOMP reason codes, PINGREQ/PINGRESP timing,
 * topic-alias usage.
 */
#ifndef TRUSTMQTT_PLUGIN_H
#define TRUSTMQTT_PLUGIN_H

#include <stddef.h>
#include <stdint.h>

#define TMQ_RING_SIZE 8192
#define TMQ_MAX_CLIENT_ID_LEN 128
#define TMQ_STATS_EVENT_PERIOD_S 10

typedef enum {
    TMQ_MODE_ENFORCE = 0,
    TMQ_MODE_MONITOR = 1,
    TMQ_MODE_FINGERPRINT = 2
} tmq_mode_t;

typedef enum {
    TMQ_VERDICT_ALLOW = 0,
    TMQ_VERDICT_WATCH = 1,
    TMQ_VERDICT_THROTTLE = 2,
    TMQ_VERDICT_QUARANTINE = 3,
    TMQ_VERDICT_KICK = 4
} tmq_verdict_level_t;

typedef struct {
    char redis_host[256];
    int redis_port;
    int emit_batch_ms;
    int verdict_refresh_ms;
    tmq_mode_t mode;
    int payload_hash_enabled;
} tmq_config_t;

/* Monotonic-ish wall clock in fractional unix seconds, used throughout the
 * plugin (event timestamps, token-bucket refill, TTL decay). */
double tmq_now(void);

#endif /* TRUSTMQTT_PLUGIN_H */
