/* core.c — broker-neutral TrustMQTT runtime.
 *
 * Resolved-semantic-event analysis only. Broker adapters supply the fields
 * their public APIs expose; absent fields stay absent from the normalized
 * event. Broker callbacks only touch in-memory state. Redis I/O happens on
 * the emitter and maintenance threads owned by this module.
 */
#include "trustmqtt_core.h"

#include "emitter.h"
#include "ring.h"
#include "verdict_cache.h"

#include <cjson/cJSON.h>
#include <hiredis/hiredis.h>
#include <openssl/evp.h>

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define TMQ_MAX_TRACKED_CLIENTS 4096
#define TMQ_CLIENT_HASH_BUCKETS 2048
#define TMQ_VERDICT_BACKOFF_START_MS 250
#define TMQ_VERDICT_BACKOFF_CAP_MS 5000

typedef struct {
    double last_activity_ts;
    double last_ka_gap_emit_ts;
    int keepalive;
} tmq_client_state_t;

typedef struct tmq_client_node {
    char client_id[TMQ_MAX_CLIENT_ID_LEN];
    tmq_client_state_t state;
    struct tmq_client_node *next;
} tmq_client_node_t;

typedef struct {
    tmq_client_node_t *buckets[TMQ_CLIENT_HASH_BUCKETS];
    size_t count;
    pthread_mutex_t lock;
} tmq_client_registry_t;

struct tmq_core {
    tmq_config_t config;
    tmq_ring_t ring;
    tmq_emitter_t *emitter;
    tmq_verdict_cache_t verdict_cache;
    tmq_client_registry_t registry;
    redisContext *verdict_redis_ctx;
    long verdict_backoff_ms;
    double next_verdict_retry_ts;
    double last_verdict_refresh_ts;
    double last_stats_emit_ts;
    tmq_core_kick_cb_t kick_cb;
    void *kick_userdata;
    pthread_t maintenance_thread;
    volatile int maintenance_running;
    pthread_mutex_t maintenance_lock;
    char (*snapshot_ids)[TMQ_MAX_CLIENT_ID_LEN];
};

double tmq_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static unsigned long tmq_hash_str(const char *s)
{
    unsigned long hash = 5381;
    int c;
    while ((c = (unsigned char)*s++)) {
        hash = ((hash << 5) + hash) + (unsigned long)c;
    }
    return hash;
}

static void registry_init(tmq_client_registry_t *reg)
{
    memset(reg->buckets, 0, sizeof(reg->buckets));
    reg->count = 0;
    pthread_mutex_init(&reg->lock, NULL);
}

static void registry_destroy(tmq_client_registry_t *reg)
{
    pthread_mutex_lock(&reg->lock);
    for (size_t i = 0; i < TMQ_CLIENT_HASH_BUCKETS; i++) {
        tmq_client_node_t *node = reg->buckets[i];
        while (node) {
            tmq_client_node_t *next = node->next;
            free(node);
            node = next;
        }
        reg->buckets[i] = NULL;
    }
    reg->count = 0;
    pthread_mutex_unlock(&reg->lock);
    pthread_mutex_destroy(&reg->lock);
}

static tmq_client_node_t *registry_find_locked(tmq_client_registry_t *reg,
                                               const char *client_id)
{
    size_t bucket = tmq_hash_str(client_id) % TMQ_CLIENT_HASH_BUCKETS;
    for (tmq_client_node_t *node = reg->buckets[bucket]; node; node = node->next) {
        if (strncmp(node->client_id, client_id, TMQ_MAX_CLIENT_ID_LEN) == 0) {
            return node;
        }
    }
    return NULL;
}

static void registry_connect(tmq_client_registry_t *reg, const char *client_id,
                             int keepalive, double now)
{
    pthread_mutex_lock(&reg->lock);
    tmq_client_node_t *node = registry_find_locked(reg, client_id);
    if (!node) {
        if (reg->count >= TMQ_MAX_TRACKED_CLIENTS) {
            pthread_mutex_unlock(&reg->lock);
            return;
        }
        node = calloc(1, sizeof(*node));
        if (!node) {
            pthread_mutex_unlock(&reg->lock);
            return;
        }
        strncpy(node->client_id, client_id, TMQ_MAX_CLIENT_ID_LEN - 1);
        size_t bucket = tmq_hash_str(client_id) % TMQ_CLIENT_HASH_BUCKETS;
        node->next = reg->buckets[bucket];
        reg->buckets[bucket] = node;
        reg->count++;
    }
    node->state.keepalive = keepalive > 0 ? keepalive : 0;
    node->state.last_activity_ts = now;
    node->state.last_ka_gap_emit_ts = 0;
    pthread_mutex_unlock(&reg->lock);
}

static void registry_touch(tmq_client_registry_t *reg, const char *client_id, double now)
{
    pthread_mutex_lock(&reg->lock);
    tmq_client_node_t *node = registry_find_locked(reg, client_id);
    if (node) {
        node->state.last_activity_ts = now;
    }
    pthread_mutex_unlock(&reg->lock);
}

static void registry_remove(tmq_client_registry_t *reg, const char *client_id)
{
    pthread_mutex_lock(&reg->lock);
    size_t bucket = tmq_hash_str(client_id) % TMQ_CLIENT_HASH_BUCKETS;
    tmq_client_node_t **cursor = &reg->buckets[bucket];
    while (*cursor) {
        if (strncmp((*cursor)->client_id, client_id, TMQ_MAX_CLIENT_ID_LEN) == 0) {
            tmq_client_node_t *dead = *cursor;
            *cursor = dead->next;
            free(dead);
            reg->count--;
            break;
        }
        cursor = &(*cursor)->next;
    }
    pthread_mutex_unlock(&reg->lock);
}

static size_t registry_snapshot_ids(tmq_client_registry_t *reg,
                                    char ids[][TMQ_MAX_CLIENT_ID_LEN], size_t max)
{
    size_t count = 0;
    pthread_mutex_lock(&reg->lock);
    for (size_t i = 0; i < TMQ_CLIENT_HASH_BUCKETS && count < max; i++) {
        for (tmq_client_node_t *node = reg->buckets[i]; node && count < max; node = node->next) {
            strncpy(ids[count], node->client_id, TMQ_MAX_CLIENT_ID_LEN - 1);
            ids[count][TMQ_MAX_CLIENT_ID_LEN - 1] = '\0';
            count++;
        }
    }
    pthread_mutex_unlock(&reg->lock);
    return count;
}

static cJSON *new_event(tmq_core_t *core, const char *event, double ts)
{
    cJSON *root = cJSON_CreateObject();
    if (!root) {
        return NULL;
    }
    cJSON_AddNumberToObject(root, "v", 1);
    cJSON_AddNumberToObject(root, "ts", ts > 0 ? ts : tmq_now());
    cJSON_AddStringToObject(root, "event", event);
    cJSON_AddStringToObject(root, "broker", core->config.broker);
    cJSON_AddStringToObject(root, "broker_id", core->config.broker_id);
    return root;
}

static void emit_event(tmq_core_t *core, cJSON *root)
{
    if (!root) {
        return;
    }
    char *json = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    if (json) {
        ring_push(&core->ring, json);
    }
}

static int compute_sha256_hex(const void *data, uint32_t len, char out_hex[65])
{
    unsigned char digest[32];
    unsigned int digest_len = 0;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    if (!ctx) {
        return 0;
    }
    int ok = EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) == 1 &&
             EVP_DigestUpdate(ctx, data, len) == 1 &&
             EVP_DigestFinal_ex(ctx, digest, &digest_len) == 1;
    EVP_MD_CTX_free(ctx);
    if (!ok) {
        return 0;
    }
    for (unsigned int i = 0; i < digest_len; i++) {
        snprintf(out_hex + i * 2, 3, "%02x", digest[i]);
    }
    out_hex[64] = '\0';
    return 1;
}

tmq_mode_t tmq_core_mode(const tmq_core_t *core)
{
    return core ? core->config.mode : TMQ_MODE_MONITOR;
}

void tmq_core_on_connect(tmq_core_t *core, const tmq_connect_event_t *event)
{
    if (!core || !event || !event->client_id || !*event->client_id) {
        return;
    }
    double now = tmq_now();
    registry_connect(&core->registry, event->client_id, event->keepalive, now);

    cJSON *root = new_event(core, "connect", now);
    if (!root) return;
    cJSON_AddStringToObject(root, "client_id", event->client_id);
    if (event->username) cJSON_AddStringToObject(root, "username", event->username);
    if (event->ip) cJSON_AddStringToObject(root, "ip", event->ip);
    if (event->protocol) cJSON_AddStringToObject(root, "protocol", event->protocol);
    if (event->has_clean_session) cJSON_AddBoolToObject(root, "clean_session", event->clean_session);
    if (event->keepalive >= 0) cJSON_AddNumberToObject(root, "keepalive", event->keepalive);
    emit_event(core, root);
}

void tmq_core_on_disconnect(tmq_core_t *core, const char *client_id,
                            int reason, int has_reason, int offline)
{
    if (!core || !client_id || !*client_id) {
        return;
    }
    registry_remove(&core->registry, client_id);
    cJSON *root = new_event(core, offline ? "client_offline" : "disconnect", tmq_now());
    if (!root) return;
    cJSON_AddStringToObject(root, "client_id", client_id);
    if (has_reason) cJSON_AddNumberToObject(root, "reason", reason);
    emit_event(core, root);
}

void tmq_core_on_publish(tmq_core_t *core, const tmq_publish_event_t *event)
{
    if (!core || !event || !event->client_id || !*event->client_id) {
        return;
    }
    double now = tmq_now();
    registry_touch(&core->registry, event->client_id, now);
    cJSON *root = new_event(core, "publish", now);
    if (!root) return;
    cJSON_AddStringToObject(root, "client_id", event->client_id);
    cJSON_AddStringToObject(root, "topic", event->topic ? event->topic : "");
    cJSON_AddNumberToObject(root, "qos", event->qos);
    cJSON_AddBoolToObject(root, "retain", event->retain);
    cJSON_AddNumberToObject(root, "payload_len", event->payload_len);

    if (core->config.payload_hash_enabled && event->payload && event->payload_len > 0) {
        char hexhash[65];
        if (compute_sha256_hex(event->payload, event->payload_len, hexhash)) {
            cJSON_AddStringToObject(root, "payload_sha256", hexhash);
        }
    }
    if (event->content_type || event->message_expiry || event->user_prop_count > 0) {
        cJSON *props = cJSON_AddObjectToObject(root, "props");
        if (event->content_type) cJSON_AddStringToObject(props, "content_type", event->content_type);
        if (event->message_expiry) cJSON_AddNumberToObject(props, "message_expiry", event->message_expiry);
        cJSON_AddNumberToObject(props, "user_prop_count", event->user_prop_count);
    }
    emit_event(core, root);
}

static void on_subscription(tmq_core_t *core, const char *event_name,
                            const char *client_id, const char *topic,
                            int qos, int sub_count)
{
    if (!core || !client_id || !*client_id) {
        return;
    }
    registry_touch(&core->registry, client_id, tmq_now());
    cJSON *root = new_event(core, event_name, tmq_now());
    if (!root) return;
    cJSON_AddStringToObject(root, "client_id", client_id);
    cJSON_AddStringToObject(root, "topic", topic ? topic : "");
    if (qos >= 0) cJSON_AddNumberToObject(root, "qos", qos);
    if (sub_count >= 0) cJSON_AddNumberToObject(root, "sub_count", sub_count);
    emit_event(core, root);
}

void tmq_core_on_subscribe(tmq_core_t *core, const char *client_id,
                           const char *topic, int qos, int sub_count)
{
    on_subscription(core, "subscribe", client_id, topic, qos, sub_count);
}

void tmq_core_on_unsubscribe(tmq_core_t *core, const char *client_id,
                             const char *topic, int qos, int sub_count)
{
    on_subscription(core, "unsubscribe", client_id, topic, qos, sub_count);
}

void tmq_core_on_auth_observe(tmq_core_t *core, const char *client_id,
                              const char *username, const char *ip)
{
    if (!core || !client_id || !*client_id) {
        return;
    }
    cJSON *root = new_event(core, "auth_observe", tmq_now());
    if (!root) return;
    cJSON_AddStringToObject(root, "client_id", client_id);
    if (username) cJSON_AddStringToObject(root, "username", username);
    if (ip) cJSON_AddStringToObject(root, "ip", ip);
    emit_event(core, root);
}

tmq_enforce_result_t tmq_core_check_access(tmq_core_t *core,
                                            const char *client_id,
                                            const char *topic,
                                            tmq_access_t access)
{
    if (!core || !client_id || !*client_id || core->config.mode == TMQ_MODE_FINGERPRINT) {
        return TMQ_ENFORCE_DEFER;
    }
    tmq_enforce_result_t result = tmq_enforce_check(&core->verdict_cache, client_id,
                                                     topic ? topic : "", access, tmq_now());
    if (result == TMQ_ENFORCE_DENY && core->config.mode == TMQ_MODE_MONITOR) {
        fprintf(stderr, "TMQ-WOULD: broker=%s client=%s topic=%s access=%d\n",
                core->config.broker, client_id, topic ? topic : "", (int)access);
        return TMQ_ENFORCE_DEFER;
    }
    return result;
}

static void scan_ka_gaps(tmq_core_t *core, double now)
{
    pthread_mutex_lock(&core->registry.lock);
    for (size_t i = 0; i < TMQ_CLIENT_HASH_BUCKETS; i++) {
        for (tmq_client_node_t *node = core->registry.buckets[i]; node; node = node->next) {
            int keepalive = node->state.keepalive;
            if (keepalive <= 0) continue;
            double gap = now - node->state.last_activity_ts;
            if (gap > 1.5 * keepalive &&
                (now - node->state.last_ka_gap_emit_ts) >= keepalive) {
                node->state.last_ka_gap_emit_ts = now;
                cJSON *root = new_event(core, "ka_gap", now);
                if (!root) continue;
                cJSON_AddStringToObject(root, "client_id", node->client_id);
                cJSON_AddNumberToObject(root, "gap_s", gap);
                cJSON_AddNumberToObject(root, "keepalive", keepalive);
                emit_event(core, root);
            }
        }
    }
    pthread_mutex_unlock(&core->registry.lock);
}

static void core_kick_client(const char *client_id, void *userdata)
{
    tmq_core_t *core = userdata;
    if (!core->kick_cb) {
        return;
    }
    core->kick_cb(client_id, core->kick_userdata);
    cJSON *root = new_event(core, "enforcement", tmq_now());
    if (!root) return;
    cJSON_AddStringToObject(root, "client_id", client_id);
    emit_event(core, root);
}

static void close_verdict_connection(tmq_core_t *core)
{
    if (core->verdict_redis_ctx) {
        redisFree(core->verdict_redis_ctx);
        core->verdict_redis_ctx = NULL;
    }
}

static void refresh_verdicts(tmq_core_t *core, double now)
{
    size_t count = registry_snapshot_ids(&core->registry, core->snapshot_ids,
                                         TMQ_MAX_TRACKED_CLIENTS);
    if (count == 0 || now < core->next_verdict_retry_ts) {
        return;
    }
    if (!core->verdict_redis_ctx) {
        struct timeval timeout = {1, 0};
        redisContext *ctx = redisConnectWithTimeout(core->config.redis_host,
                                                    core->config.redis_port, timeout);
        if (!ctx || ctx->err) {
            if (ctx) redisFree(ctx);
            core->next_verdict_retry_ts = now + core->verdict_backoff_ms / 1000.0;
            core->verdict_backoff_ms = core->verdict_backoff_ms * 2 > TMQ_VERDICT_BACKOFF_CAP_MS
                                           ? TMQ_VERDICT_BACKOFF_CAP_MS
                                           : core->verdict_backoff_ms * 2;
            return;
        }
        core->verdict_redis_ctx = ctx;
        core->verdict_backoff_ms = TMQ_VERDICT_BACKOFF_START_MS;
        core->next_verdict_retry_ts = 0;
    }

    redisContext *ctx = core->verdict_redis_ctx;
    size_t appended = 0;
    for (; appended < count; appended++) {
        if (redisAppendCommand(ctx, "GET tmq:verdictp:%s", core->snapshot_ids[appended]) != REDIS_OK) {
            break;
        }
    }
    int broken = appended != count;
    for (size_t i = 0; i < appended; i++) {
        redisReply *reply = NULL;
        if (redisGetReply(ctx, (void **)&reply) != REDIS_OK) {
            broken = 1;
        } else if (reply && reply->type == REDIS_REPLY_STRING) {
            int level = 0;
            float score = 0;
            float rate = 0;
            long expires_at = 0;
            if (sscanf(reply->str, "%d|%f|%ld|%f", &level, &score, &expires_at, &rate) == 4) {
                verdict_cache_upsert(&core->verdict_cache, core->snapshot_ids[i],
                                     (uint8_t)level, score, (double)expires_at, rate, now);
            }
        }
        if (reply) freeReplyObject(reply);
    }
    if (broken) {
        close_verdict_connection(core);
    }
}

void tmq_core_maintenance(tmq_core_t *core)
{
    if (!core || pthread_mutex_trylock(&core->maintenance_lock) != 0) {
        return;
    }
    double now = tmq_now();
    scan_ka_gaps(core, now);
    verdict_cache_decay_pass(&core->verdict_cache, now);

    if (core->config.mode != TMQ_MODE_FINGERPRINT) {
        double refresh_s = core->config.verdict_refresh_ms / 1000.0;
        if (now - core->last_verdict_refresh_ts >= refresh_s) {
            refresh_verdicts(core, now);
            core->last_verdict_refresh_ts = now;
        }
    }
    if (now - core->last_stats_emit_ts >= TMQ_STATS_EVENT_PERIOD_S) {
        cJSON *root = new_event(core, "plugin_stats", now);
        if (root) {
            cJSON_AddNumberToObject(root, "dropped_events",
                                    (double)ring_dropped_count(&core->ring));
            cJSON_AddNumberToObject(root, "ring_size", (double)ring_size(&core->ring));
            emit_event(core, root);
        }
        core->last_stats_emit_ts = now;
    }
    pthread_mutex_unlock(&core->maintenance_lock);
}

void tmq_core_poll_actions(tmq_core_t *core)
{
    if (!core) {
        return;
    }
    verdict_cache_process_kicks(&core->verdict_cache, core_kick_client, core);
}

static void *maintenance_loop(void *userdata)
{
    tmq_core_t *core = userdata;
    int sleep_ms = core->config.verdict_refresh_ms;
    if (sleep_ms < 25) sleep_ms = 25;
    if (sleep_ms > 100) sleep_ms = 100;
    while (core->maintenance_running) {
        usleep((useconds_t)sleep_ms * 1000);
        tmq_core_maintenance(core);
    }
    return NULL;
}

tmq_core_t *tmq_core_create(const tmq_config_t *config,
                            tmq_core_kick_cb_t kick_cb,
                            void *kick_userdata)
{
    if (!config) {
        return NULL;
    }
    tmq_core_t *core = calloc(1, sizeof(*core));
    if (!core) {
        return NULL;
    }
    core->snapshot_ids = calloc(TMQ_MAX_TRACKED_CLIENTS, sizeof(*core->snapshot_ids));
    if (!core->snapshot_ids) {
        free(core);
        return NULL;
    }
    core->config = *config;
    core->kick_cb = kick_cb;
    core->kick_userdata = kick_userdata;
    core->verdict_backoff_ms = TMQ_VERDICT_BACKOFF_START_MS;
    ring_init(&core->ring);
    verdict_cache_init(&core->verdict_cache);
    registry_init(&core->registry);
    pthread_mutex_init(&core->maintenance_lock, NULL);

    if (emitter_start(&core->emitter, &core->ring, core->config.redis_host,
                      core->config.redis_port, core->config.emit_batch_ms) != 0) {
        fprintf(stderr, "trustmqtt_core: emitter thread did not start; events will be buffered/dropped\n");
    }
    core->maintenance_running = 1;
    if (pthread_create(&core->maintenance_thread, NULL, maintenance_loop, core) != 0) {
        core->maintenance_running = 0;
        emitter_stop(core->emitter);
        pthread_mutex_destroy(&core->maintenance_lock);
        verdict_cache_destroy(&core->verdict_cache);
        registry_destroy(&core->registry);
        ring_destroy(&core->ring);
        free(core->snapshot_ids);
        free(core);
        return NULL;
    }
    return core;
}

void tmq_core_destroy(tmq_core_t *core)
{
    if (!core) {
        return;
    }
    core->maintenance_running = 0;
    pthread_join(core->maintenance_thread, NULL);
    emitter_stop(core->emitter);
    close_verdict_connection(core);
    pthread_mutex_destroy(&core->maintenance_lock);
    verdict_cache_destroy(&core->verdict_cache);
    registry_destroy(&core->registry);
    ring_destroy(&core->ring);
    free(core->snapshot_ids);
    free(core);
}
