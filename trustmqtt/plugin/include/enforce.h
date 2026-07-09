/* enforce.h — ACL_CHECK enforcement decision logic (spec §6.1). Broker-
 * agnostic: returns a tri-state result that plugin.c maps onto Mosquitto's
 * MOSQ_ERR_PLUGIN_DEFER / MOSQ_ERR_ACL_DENIED so this module stays testable
 * without linking the broker.
 */
#ifndef TMQ_ENFORCE_H
#define TMQ_ENFORCE_H

#include "verdict_cache.h"
#include "trustmqtt_plugin.h"

typedef enum {
    TMQ_ACCESS_READ = 1,
    TMQ_ACCESS_WRITE = 2,
    TMQ_ACCESS_SUBSCRIBE = 4
} tmq_access_t;

typedef enum {
    TMQ_ENFORCE_DEFER = 0,  /* let normal ACLs decide */
    TMQ_ENFORCE_DENY = 1
} tmq_enforce_result_t;

/* 1 if `topic` matches the MQTT filter `filter` (supports '+' and a
 * trailing '#'; good enough for the fixed quarantine-namespace check). */
int tmq_topic_matches(const char *filter, const char *topic);

/* Core decision per §6.1. `now` drives token-bucket refill. In `monitor`
 * mode the caller is expected to log would-be denials itself (this
 * function still returns the real verdict so callers can tell the two
 * apart; plugin.c is the one that turns DENY into DEFER+log for monitor
 * mode). */
tmq_enforce_result_t tmq_enforce_check(tmq_verdict_cache_t *cache, const char *client_id,
                                        const char *topic, tmq_access_t access, double now);

#endif /* TMQ_ENFORCE_H */
