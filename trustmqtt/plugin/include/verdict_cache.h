/* verdict_cache.h — in-memory client_id -> verdict map (spec §3.5).
 * Populated by the TICK-driven refresher reading packed strings from Redis
 * (§4.2), consulted synchronously by the ACL_CHECK enforcement path (§6).
 * Guarded by a single rwlock: reads (verdict_cache_get) take a read lock,
 * anything that mutates state (upsert/token-consume/decay/kick-scan) takes
 * a write lock.
 */
#ifndef TMQ_VERDICT_CACHE_H
#define TMQ_VERDICT_CACHE_H

#include <pthread.h>
#include <stdint.h>
#include "trustmqtt_plugin.h"

#define TMQ_VERDICT_HASH_BUCKETS 4096

typedef struct tmq_verdict_entry {
    char client_id[TMQ_MAX_CLIENT_ID_LEN];
    uint8_t level;
    float score;
    double expires_at;
    float rate;            /* THROTTLE tokens/sec */
    float tokens;          /* current token bucket level */
    double tokens_ts;       /* last refill time */
    double last_decay_ts;   /* last TTL-decay step applied */
    struct tmq_verdict_entry *next;
} tmq_verdict_entry_t;

typedef struct {
    tmq_verdict_entry_t *buckets[TMQ_VERDICT_HASH_BUCKETS];
    pthread_rwlock_t lock;
} tmq_verdict_cache_t;

void verdict_cache_init(tmq_verdict_cache_t *c);
void verdict_cache_destroy(tmq_verdict_cache_t *c);

/* Copies the current entry for client_id into *out. Returns 1 if found,
 * 0 if not (caller should treat "not found" as ALLOW). */
int verdict_cache_get(tmq_verdict_cache_t *c, const char *client_id, tmq_verdict_entry_t *out);

/* Insert or refresh a verdict from a parsed "L|S|E|R" packed string. Token
 * bucket state is preserved across refreshes for existing clients and
 * initialized full (2*rate) for newly-seen ones. */
void verdict_cache_upsert(tmq_verdict_cache_t *c, const char *client_id,
                           uint8_t level, float score, double expires_at,
                           float rate, double now);

/* Refills then attempts to consume one token for a THROTTLE client.
 * Returns 1 if a token was consumed (allow), 0 if the bucket is empty (deny).
 * If the client has no cache entry this returns 1 (fail-open). */
int verdict_cache_try_consume_token(tmq_verdict_cache_t *c, const char *client_id, double now);

/* TTL decay pass, called once per TICK: any entry whose expires_at has
 * passed loses one verdict level every 60s until it reaches ALLOW, at which
 * point it is removed from the cache entirely (fail-open under Redis
 * outage). */
void verdict_cache_decay_pass(tmq_verdict_cache_t *c, double now);

typedef void (*tmq_kick_cb_t)(const char *client_id, void *userdata);

/* Invokes cb() for every entry currently at KICK level, then demotes that
 * entry to QUARANTINE so a reconnect storm doesn't get re-kicked forever. */
void verdict_cache_process_kicks(tmq_verdict_cache_t *c, tmq_kick_cb_t cb, void *userdata);

#endif /* TMQ_VERDICT_CACHE_H */
