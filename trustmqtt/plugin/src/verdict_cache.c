#include "verdict_cache.h"
#include <stdlib.h>
#include <string.h>

static unsigned long djb2(const char *s)
{
    unsigned long hash = 5381;
    int c;
    while ((c = (unsigned char)*s++)) {
        hash = ((hash << 5) + hash) + (unsigned long)c;
    }
    return hash;
}

static size_t bucket_for(const char *client_id)
{
    return djb2(client_id) % TMQ_VERDICT_HASH_BUCKETS;
}

static tmq_verdict_entry_t *find_locked(tmq_verdict_cache_t *c, const char *client_id)
{
    tmq_verdict_entry_t *e = c->buckets[bucket_for(client_id)];
    while (e) {
        if (strncmp(e->client_id, client_id, TMQ_MAX_CLIENT_ID_LEN) == 0) {
            return e;
        }
        e = e->next;
    }
    return NULL;
}

void verdict_cache_init(tmq_verdict_cache_t *c)
{
    memset(c->buckets, 0, sizeof(c->buckets));
    pthread_rwlock_init(&c->lock, NULL);
}

void verdict_cache_destroy(tmq_verdict_cache_t *c)
{
    pthread_rwlock_wrlock(&c->lock);
    for (size_t i = 0; i < TMQ_VERDICT_HASH_BUCKETS; i++) {
        tmq_verdict_entry_t *e = c->buckets[i];
        while (e) {
            tmq_verdict_entry_t *next = e->next;
            free(e);
            e = next;
        }
        c->buckets[i] = NULL;
    }
    pthread_rwlock_unlock(&c->lock);
    pthread_rwlock_destroy(&c->lock);
}

int verdict_cache_get(tmq_verdict_cache_t *c, const char *client_id, tmq_verdict_entry_t *out)
{
    pthread_rwlock_rdlock(&c->lock);
    tmq_verdict_entry_t *e = find_locked(c, client_id);
    if (e) {
        *out = *e;
        out->next = NULL;
    }
    pthread_rwlock_unlock(&c->lock);
    return e != NULL;
}

void verdict_cache_upsert(tmq_verdict_cache_t *c, const char *client_id,
                           uint8_t level, float score, double expires_at,
                           float rate, double now)
{
    pthread_rwlock_wrlock(&c->lock);
    tmq_verdict_entry_t *e = find_locked(c, client_id);
    if (!e) {
        e = calloc(1, sizeof(tmq_verdict_entry_t));
        strncpy(e->client_id, client_id, TMQ_MAX_CLIENT_ID_LEN - 1);
        e->tokens = rate * 2.0f;
        e->tokens_ts = now;
        e->last_decay_ts = now;
        size_t b = bucket_for(client_id);
        e->next = c->buckets[b];
        c->buckets[b] = e;
    }
    e->level = level;
    e->score = score;
    e->expires_at = expires_at;
    e->rate = rate;
    pthread_rwlock_unlock(&c->lock);
}

int verdict_cache_try_consume_token(tmq_verdict_cache_t *c, const char *client_id, double now)
{
    pthread_rwlock_wrlock(&c->lock);
    tmq_verdict_entry_t *e = find_locked(c, client_id);
    if (!e) {
        pthread_rwlock_unlock(&c->lock);
        return 1; /* fail-open: no cached state, don't deny */
    }
    double elapsed = now - e->tokens_ts;
    if (elapsed > 0) {
        float cap = e->rate * 2.0f;
        e->tokens += (float)(elapsed * e->rate);
        if (e->tokens > cap) {
            e->tokens = cap;
        }
        e->tokens_ts = now;
    }
    int allowed;
    if (e->tokens >= 1.0f) {
        e->tokens -= 1.0f;
        allowed = 1;
    } else {
        allowed = 0;
    }
    pthread_rwlock_unlock(&c->lock);
    return allowed;
}

void verdict_cache_decay_pass(tmq_verdict_cache_t *c, double now)
{
    pthread_rwlock_wrlock(&c->lock);
    for (size_t i = 0; i < TMQ_VERDICT_HASH_BUCKETS; i++) {
        tmq_verdict_entry_t **pp = &c->buckets[i];
        while (*pp) {
            tmq_verdict_entry_t *e = *pp;
            if (now > e->expires_at && now - e->last_decay_ts >= 60.0) {
                if (e->level > TMQ_VERDICT_ALLOW) {
                    e->level--;
                }
                e->last_decay_ts = now;
                e->expires_at = now + 120.0;
                if (e->level == TMQ_VERDICT_ALLOW) {
                    /* Evict immediately in this same step: expires_at was
                     * just renewed above, so a later pass would never see
                     * "now > expires_at" true again to catch this. */
                    *pp = e->next;
                    free(e);
                    continue;
                }
            }
            pp = &e->next;
        }
    }
    pthread_rwlock_unlock(&c->lock);
}

void verdict_cache_process_kicks(tmq_verdict_cache_t *c, tmq_kick_cb_t cb, void *userdata)
{
    pthread_rwlock_wrlock(&c->lock);
    for (size_t i = 0; i < TMQ_VERDICT_HASH_BUCKETS; i++) {
        tmq_verdict_entry_t *e = c->buckets[i];
        while (e) {
            if (e->level == TMQ_VERDICT_KICK) {
                cb(e->client_id, userdata);
                e->level = TMQ_VERDICT_QUARANTINE;
            }
            e = e->next;
        }
    }
    pthread_rwlock_unlock(&c->lock);
}
