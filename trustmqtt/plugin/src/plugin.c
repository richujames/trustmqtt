/* plugin.c — TrustMQTT Mosquitto 2.1.x plugin entry point (spec §3).
 *
 * Resolved-semantic-event analysis only (see trustmqtt_plugin.h header
 * comment for the exact list of unobservable wire-level fields). No
 * callback in this file performs blocking network I/O: events go onto the
 * ring buffer for emitter.c's background thread (§3.3); verdict reads and
 * enforcement decisions happen off the in-memory cache (§3.5, §6), refreshed
 * synchronously but only from MOSQ_EVT_TICK, never from a hot-path callback.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <stdint.h>
#include <stdbool.h>
#include <unistd.h>

#include "trustmqtt_plugin.h"
#include "ring.h"
#include "emitter.h"
#include "verdict_cache.h"
#include "enforce.h"

#include <hiredis/hiredis.h>
/* We use the real cJSON library rather than a vendored copy: Mosquitto
 * 2.1.0's public headers (mosquitto.h -> libcommon_cjson.h) already pull in
 * and the broker already links cJSON, so using the same library guarantees
 * ABI compatibility and avoids the symbol collision a vendored reimpl would
 * cause. Serialization only — we never parse JSON here (verdicts arrive as a
 * pipe-delimited packed string, §4.2, so the hot path never parses JSON). */
#include <cjson/cJSON.h>

#ifdef HAVE_MOSQUITTO
#include <mosquitto.h>
#include <mosquitto_broker.h>
#include <mosquitto_plugin.h>
#include <openssl/evp.h>
#else
/* Fallback declarations so this file still compiles without the broker
 * headers (e.g. quick local syntax checks outside the Docker build, where
 * the real headers always are present via MOSQUITTO_BROKER_INCLUDE_DIR). */
struct mosquitto_opt { char *key; char *value; };
typedef void mosquitto_plugin_id_t;
#ifndef MOSQ_ERR_SUCCESS
#define MOSQ_ERR_SUCCESS 0
#endif
#ifndef MOSQ_ERR_ACL_DENIED
#define MOSQ_ERR_ACL_DENIED 12
#endif
#ifndef MOSQ_ERR_PLUGIN_DEFER
#define MOSQ_ERR_PLUGIN_DEFER (-2)
#endif
#endif

#define TMQ_MAX_TRACKED_CLIENTS 4096
#define TMQ_CLIENT_HASH_BUCKETS 2048
#define TMQ_VERDICT_BACKOFF_START_MS 250
#define TMQ_VERDICT_BACKOFF_CAP_MS 5000

/* ---------------------------------------------------------------------- *
 * Per-client bookkeeping registry: keepalive conformance (§3.4) and the
 * "which client_ids do we need verdicts for" list consumed by the TICK
 * refresher (§3.5). Populated on CONNECT, cleared on DISCONNECT/OFFLINE.
 * ---------------------------------------------------------------------- */
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
    pthread_mutex_t lock;
} tmq_client_registry_t;

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
    pthread_mutex_init(&reg->lock, NULL);
}

static void registry_destroy(tmq_client_registry_t *reg)
{
    pthread_mutex_lock(&reg->lock);
    for (size_t i = 0; i < TMQ_CLIENT_HASH_BUCKETS; i++) {
        tmq_client_node_t *n = reg->buckets[i];
        while (n) {
            tmq_client_node_t *next = n->next;
            free(n);
            n = next;
        }
        reg->buckets[i] = NULL;
    }
    pthread_mutex_unlock(&reg->lock);
    pthread_mutex_destroy(&reg->lock);
}

static tmq_client_node_t *registry_find_locked(tmq_client_registry_t *reg, const char *client_id)
{
    tmq_client_node_t *n = reg->buckets[tmq_hash_str(client_id) % TMQ_CLIENT_HASH_BUCKETS];
    while (n) {
        if (strncmp(n->client_id, client_id, TMQ_MAX_CLIENT_ID_LEN) == 0) {
            return n;
        }
        n = n->next;
    }
    return NULL;
}

static void registry_connect(tmq_client_registry_t *reg, const char *client_id, int keepalive, double now)
{
    pthread_mutex_lock(&reg->lock);
    tmq_client_node_t *n = registry_find_locked(reg, client_id);
    if (!n) {
        n = calloc(1, sizeof(tmq_client_node_t));
        strncpy(n->client_id, client_id, TMQ_MAX_CLIENT_ID_LEN - 1);
        size_t b = tmq_hash_str(client_id) % TMQ_CLIENT_HASH_BUCKETS;
        n->next = reg->buckets[b];
        reg->buckets[b] = n;
    }
    n->state.keepalive = keepalive;
    n->state.last_activity_ts = now;
    n->state.last_ka_gap_emit_ts = 0;
    pthread_mutex_unlock(&reg->lock);
}

static void registry_touch_activity(tmq_client_registry_t *reg, const char *client_id, double now)
{
    pthread_mutex_lock(&reg->lock);
    tmq_client_node_t *n = registry_find_locked(reg, client_id);
    if (n) {
        n->state.last_activity_ts = now;
    }
    pthread_mutex_unlock(&reg->lock);
}

static void registry_remove(tmq_client_registry_t *reg, const char *client_id)
{
    pthread_mutex_lock(&reg->lock);
    size_t b = tmq_hash_str(client_id) % TMQ_CLIENT_HASH_BUCKETS;
    tmq_client_node_t **pp = &reg->buckets[b];
    while (*pp) {
        if (strncmp((*pp)->client_id, client_id, TMQ_MAX_CLIENT_ID_LEN) == 0) {
            tmq_client_node_t *dead = *pp;
            *pp = dead->next;
            free(dead);
            break;
        }
        pp = &(*pp)->next;
    }
    pthread_mutex_unlock(&reg->lock);
}

static size_t registry_snapshot_ids(tmq_client_registry_t *reg, char ids[][TMQ_MAX_CLIENT_ID_LEN], size_t max)
{
    size_t n = 0;
    pthread_mutex_lock(&reg->lock);
    for (size_t i = 0; i < TMQ_CLIENT_HASH_BUCKETS && n < max; i++) {
        tmq_client_node_t *node = reg->buckets[i];
        while (node && n < max) {
            strncpy(ids[n], node->client_id, TMQ_MAX_CLIENT_ID_LEN - 1);
            ids[n][TMQ_MAX_CLIENT_ID_LEN - 1] = '\0';
            n++;
            node = node->next;
        }
    }
    pthread_mutex_unlock(&reg->lock);
    return n;
}

/* ---------------------------------------------------------------------- *
 * Global plugin state
 * ---------------------------------------------------------------------- */
typedef struct {
    tmq_config_t config;
    tmq_ring_t ring;
    tmq_emitter_t *emitter;
    tmq_verdict_cache_t verdict_cache;
    tmq_client_registry_t registry;
    redisContext *verdict_redis_ctx;
    long verdict_backoff_ms;
    double last_verdict_refresh_ts;
    double last_stats_emit_ts;
#ifdef HAVE_MOSQUITTO
    mosquitto_plugin_id_t *mosq_plugin_id;
#endif
} tmq_plugin_state_t;

static tmq_plugin_state_t g_state;

double tmq_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static void emit_event(cJSON *root)
{
    char *json = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    ring_push(&g_state.ring, json); /* takes ownership of json */
}

#ifdef HAVE_MOSQUITTO
static const char *protocol_name(int protocol)
{
    switch (protocol) {
        case mp_mqtt: return "mqtt";
        case mp_mqttsn: return "mqttsn";
        case mp_websockets: return "websockets";
        case mp_http_api: return "http_api";
        default: return "unknown";
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
    return 1;
}

static int count_user_properties(const mosquitto_property *proplist)
{
    int count = 0;
    bool skip_first = false;
    const mosquitto_property *cur = proplist;
    while (cur) {
        char *name = NULL, *value = NULL;
        cur = mosquitto_property_read_string_pair(cur, MQTT_PROP_USER_PROPERTY, &name, &value, skip_first);
        if (!cur) {
            break;
        }
        count++;
        free(name);
        free(value);
        skip_first = true;
    }
    return count;
}

/* ---------------------------------------------------------------------- *
 * Event callbacks (§3.2)
 * ---------------------------------------------------------------------- */
static int handle_connect(int event, void *event_data, void *userdata)
{
    (void)event; (void)userdata;
    struct mosquitto_evt_connect *ed = event_data;
    const char *cid = mosquitto_client_id(ed->client);
    if (!cid) {
        return MOSQ_ERR_SUCCESS;
    }
    const char *username = mosquitto_client_username(ed->client);
    const char *ip = mosquitto_client_address(ed->client);
    int protocol = mosquitto_client_protocol(ed->client);
    bool clean_session = mosquitto_client_clean_session(ed->client);
    int keepalive = mosquitto_client_keepalive(ed->client);
    double now = tmq_now();

    registry_connect(&g_state.registry, cid, keepalive, now);

    cJSON *root = cJSON_CreateObject();
    cJSON_AddNumberToObject(root, "v", 1);
    cJSON_AddNumberToObject(root, "ts", now);
    cJSON_AddStringToObject(root, "event", "connect");
    cJSON_AddStringToObject(root, "client_id", cid);
    if (username) cJSON_AddStringToObject(root, "username", username);
    if (ip) cJSON_AddStringToObject(root, "ip", ip);
    cJSON_AddStringToObject(root, "protocol", protocol_name(protocol));
    cJSON_AddBoolToObject(root, "clean_session", clean_session);
    cJSON_AddNumberToObject(root, "keepalive", keepalive);
    emit_event(root);

    return MOSQ_ERR_SUCCESS;
}

static int handle_disconnect(int event, void *event_data, void *userdata)
{
    (void)event; (void)userdata;
    struct mosquitto_evt_disconnect *ed = event_data;
    const char *cid = mosquitto_client_id(ed->client);
    if (!cid) {
        return MOSQ_ERR_SUCCESS;
    }
    registry_remove(&g_state.registry, cid);

    cJSON *root = cJSON_CreateObject();
    cJSON_AddNumberToObject(root, "v", 1);
    cJSON_AddNumberToObject(root, "ts", tmq_now());
    cJSON_AddStringToObject(root, "event", "disconnect");
    cJSON_AddStringToObject(root, "client_id", cid);
    cJSON_AddNumberToObject(root, "reason", ed->reason);
    emit_event(root);

    return MOSQ_ERR_SUCCESS;
}

static int handle_client_offline(int event, void *event_data, void *userdata)
{
    (void)event; (void)userdata;
    struct mosquitto_evt_client_offline *ed = event_data;
    const char *cid = mosquitto_client_id(ed->client);
    if (!cid) {
        return MOSQ_ERR_SUCCESS;
    }
    registry_remove(&g_state.registry, cid);

    cJSON *root = cJSON_CreateObject();
    cJSON_AddNumberToObject(root, "v", 1);
    cJSON_AddNumberToObject(root, "ts", tmq_now());
    cJSON_AddStringToObject(root, "event", "client_offline");
    cJSON_AddStringToObject(root, "client_id", cid);
    emit_event(root);

    return MOSQ_ERR_SUCCESS;
}

static int handle_message_in(int event, void *event_data, void *userdata)
{
    (void)event; (void)userdata;
    struct mosquitto_evt_message *ed = event_data;
    const char *cid = mosquitto_client_id(ed->client);
    if (!cid) {
        return MOSQ_ERR_SUCCESS;
    }
    double now = tmq_now();
    registry_touch_activity(&g_state.registry, cid, now);

    cJSON *root = cJSON_CreateObject();
    cJSON_AddNumberToObject(root, "v", 1);
    cJSON_AddNumberToObject(root, "ts", now);
    cJSON_AddStringToObject(root, "event", "publish");
    cJSON_AddStringToObject(root, "client_id", cid);
    cJSON_AddStringToObject(root, "topic", ed->topic ? ed->topic : "");
    cJSON_AddNumberToObject(root, "qos", ed->qos);
    cJSON_AddBoolToObject(root, "retain", ed->retain);
    cJSON_AddNumberToObject(root, "payload_len", ed->payloadlen);

    if (g_state.config.payload_hash_enabled && ed->payload && ed->payloadlen > 0) {
        char hexhash[65];
        if (compute_sha256_hex(ed->payload, ed->payloadlen, hexhash)) {
            cJSON_AddStringToObject(root, "payload_sha256", hexhash);
        }
    }

    char *content_type = NULL;
    uint32_t message_expiry = 0;
    int user_prop_count = 0;
    if (ed->properties) {
        mosquitto_property_read_string(ed->properties, MQTT_PROP_CONTENT_TYPE, &content_type, false);
        mosquitto_property_read_int32(ed->properties, MQTT_PROP_MESSAGE_EXPIRY_INTERVAL, &message_expiry, false);
        user_prop_count = count_user_properties(ed->properties);
    }
    if (content_type || message_expiry || user_prop_count) {
        cJSON *props = cJSON_AddObjectToObject(root, "props");
        if (content_type) cJSON_AddStringToObject(props, "content_type", content_type);
        if (message_expiry) cJSON_AddNumberToObject(props, "message_expiry", message_expiry);
        cJSON_AddNumberToObject(props, "user_prop_count", user_prop_count);
    }
    free(content_type);

    emit_event(root);
    return MOSQ_ERR_SUCCESS;
}

static int emit_sub_event(const char *event_name, const char *cid, const char *topic, int qos, int sub_count)
{
    cJSON *root = cJSON_CreateObject();
    cJSON_AddNumberToObject(root, "v", 1);
    cJSON_AddNumberToObject(root, "ts", tmq_now());
    cJSON_AddStringToObject(root, "event", event_name);
    cJSON_AddStringToObject(root, "client_id", cid);
    cJSON_AddStringToObject(root, "topic", topic ? topic : "");
    cJSON_AddNumberToObject(root, "qos", qos);
    cJSON_AddNumberToObject(root, "sub_count", sub_count);
    emit_event(root);
    return MOSQ_ERR_SUCCESS;
}

static int handle_subscribe(int event, void *event_data, void *userdata)
{
    (void)event; (void)userdata;
    struct mosquitto_evt_subscribe *ed = event_data;
    const char *cid = mosquitto_client_id(ed->client);
    if (!cid) {
        return MOSQ_ERR_SUCCESS;
    }
    registry_touch_activity(&g_state.registry, cid, tmq_now());
    int qos = ed->data.options & 0x03;
    int sub_count = mosquitto_client_sub_count(ed->client);
    return emit_sub_event("subscribe", cid, ed->data.topic_filter, qos, sub_count);
}

static int handle_unsubscribe(int event, void *event_data, void *userdata)
{
    (void)event; (void)userdata;
    struct mosquitto_evt_unsubscribe *ed = event_data;
    const char *cid = mosquitto_client_id(ed->client);
    if (!cid) {
        return MOSQ_ERR_SUCCESS;
    }
    registry_touch_activity(&g_state.registry, cid, tmq_now());
    int qos = ed->data.options & 0x03;
    int sub_count = mosquitto_client_sub_count(ed->client);
    return emit_sub_event("unsubscribe", cid, ed->data.topic_filter, qos, sub_count);
}

static int handle_basic_auth(int event, void *event_data, void *userdata)
{
    /* Observation only — we never decide auth, so this always defers. */
    (void)event; (void)userdata;
    struct mosquitto_evt_basic_auth *ed = event_data;
    const char *cid = mosquitto_client_id(ed->client);
    const char *ip = mosquitto_client_address(ed->client);

    cJSON *root = cJSON_CreateObject();
    cJSON_AddNumberToObject(root, "v", 1);
    cJSON_AddNumberToObject(root, "ts", tmq_now());
    cJSON_AddStringToObject(root, "event", "auth_observe");
    cJSON_AddStringToObject(root, "client_id", cid ? cid : "");
    if (ed->username) cJSON_AddStringToObject(root, "username", ed->username);
    if (ip) cJSON_AddStringToObject(root, "ip", ip);
    emit_event(root);

    return MOSQ_ERR_PLUGIN_DEFER;
}

static int handle_acl_check(int event, void *event_data, void *userdata)
{
    (void)event; (void)userdata;
    struct mosquitto_evt_acl_check *ed = event_data;
    const char *cid = mosquitto_client_id(ed->client);
    if (!cid) {
        return MOSQ_ERR_PLUGIN_DEFER;
    }

    if (g_state.config.mode == TMQ_MODE_FINGERPRINT) {
        /* §3.6: fingerprint mode never fetches verdicts or enforces. */
        return MOSQ_ERR_PLUGIN_DEFER;
    }

    tmq_access_t access;
    if (ed->access & MOSQ_ACL_SUBSCRIBE) {
        access = TMQ_ACCESS_SUBSCRIBE;
    } else if (ed->access & MOSQ_ACL_WRITE) {
        access = TMQ_ACCESS_WRITE;
    } else {
        access = TMQ_ACCESS_READ;
    }

    double now = tmq_now();
    tmq_enforce_result_t result = tmq_enforce_check(&g_state.verdict_cache, cid,
                                                     ed->topic ? ed->topic : "", access, now);

    if (result == TMQ_ENFORCE_DENY) {
        if (g_state.config.mode == TMQ_MODE_MONITOR) {
            fprintf(stderr, "TMQ-WOULD: deny client=%s topic=%s access=%d\n",
                    cid, ed->topic ? ed->topic : "", ed->access);
            return MOSQ_ERR_PLUGIN_DEFER;
        }
        return MOSQ_ERR_ACL_DENIED;
    }
    return MOSQ_ERR_PLUGIN_DEFER;
}

static void kick_client_cb(const char *client_id, void *userdata)
{
    (void)userdata;
    mosquitto_kick_client_by_clientid(client_id, false);

    cJSON *root = cJSON_CreateObject();
    cJSON_AddNumberToObject(root, "v", 1);
    cJSON_AddNumberToObject(root, "ts", tmq_now());
    cJSON_AddStringToObject(root, "event", "enforcement");
    cJSON_AddStringToObject(root, "client_id", client_id);
    emit_event(root);
}

/* §3.4 keepalive conformance: emit at most one ka_gap per keepalive
 * interval while a client stays silent-but-connected. */
static void scan_ka_gaps(tmq_client_registry_t *reg, double now)
{
    pthread_mutex_lock(&reg->lock);
    for (size_t i = 0; i < TMQ_CLIENT_HASH_BUCKETS; i++) {
        for (tmq_client_node_t *n = reg->buckets[i]; n; n = n->next) {
            if (n->state.keepalive <= 0) {
                continue;
            }
            double gap = now - n->state.last_activity_ts;
            if (gap > 1.5 * n->state.keepalive &&
                (now - n->state.last_ka_gap_emit_ts) >= n->state.keepalive) {
                n->state.last_ka_gap_emit_ts = now;
                cJSON *root = cJSON_CreateObject();
                cJSON_AddNumberToObject(root, "v", 1);
                cJSON_AddNumberToObject(root, "ts", now);
                cJSON_AddStringToObject(root, "event", "ka_gap");
                cJSON_AddStringToObject(root, "client_id", n->client_id);
                cJSON_AddNumberToObject(root, "gap_s", gap);
                cJSON_AddNumberToObject(root, "keepalive", n->state.keepalive);
                emit_event(root);
            }
        }
    }
    pthread_mutex_unlock(&reg->lock);
}

static void refresh_verdicts_from_redis(double now)
{
    static char ids[TMQ_MAX_TRACKED_CLIENTS][TMQ_MAX_CLIENT_ID_LEN];
    size_t n = registry_snapshot_ids(&g_state.registry, ids, TMQ_MAX_TRACKED_CLIENTS);
    if (n == 0) {
        return;
    }

    if (!g_state.verdict_redis_ctx) {
        struct timeval tv = {1, 0};
        redisContext *ctx = redisConnectWithTimeout(g_state.config.redis_host, g_state.config.redis_port, tv);
        if (!ctx || ctx->err) {
            if (ctx) redisFree(ctx);
            usleep((useconds_t)g_state.verdict_backoff_ms * 1000);
            g_state.verdict_backoff_ms = g_state.verdict_backoff_ms * 2 > TMQ_VERDICT_BACKOFF_CAP_MS
                                              ? TMQ_VERDICT_BACKOFF_CAP_MS
                                              : g_state.verdict_backoff_ms * 2;
            return; /* fail-open: cache decays on its own TTL logic */
        }
        g_state.verdict_redis_ctx = ctx;
        g_state.verdict_backoff_ms = TMQ_VERDICT_BACKOFF_START_MS;
    }
    redisContext *ctx = g_state.verdict_redis_ctx;

    for (size_t i = 0; i < n; i++) {
        redisAppendCommand(ctx, "GET tmq:verdictp:%s", ids[i]);
    }
    int broken = 0;
    for (size_t i = 0; i < n; i++) {
        redisReply *reply = NULL;
        if (redisGetReply(ctx, (void **)&reply) != REDIS_OK) {
            broken = 1;
        } else if (reply && reply->type == REDIS_REPLY_STRING) {
            int level = 0;
            float score = 0, rate = 0;
            long expires_at_l = 0;
            if (sscanf(reply->str, "%d|%f|%ld|%f", &level, &score, &expires_at_l, &rate) == 4) {
                verdict_cache_upsert(&g_state.verdict_cache, ids[i], (uint8_t)level, score,
                                      (double)expires_at_l, rate, now);
            }
        }
        if (reply) {
            freeReplyObject(reply);
        }
    }
    if (broken) {
        redisFree(ctx);
        g_state.verdict_redis_ctx = NULL;
    }
}

static int handle_tick(int event, void *event_data, void *userdata)
{
    (void)event; (void)event_data; (void)userdata;
    double now = tmq_now();

    scan_ka_gaps(&g_state.registry, now);
    verdict_cache_decay_pass(&g_state.verdict_cache, now);
    verdict_cache_process_kicks(&g_state.verdict_cache, kick_client_cb, NULL);

    if (g_state.config.mode != TMQ_MODE_FINGERPRINT) {
        double refresh_period_s = g_state.config.verdict_refresh_ms / 1000.0;
        if (now - g_state.last_verdict_refresh_ts >= refresh_period_s) {
            refresh_verdicts_from_redis(now);
            g_state.last_verdict_refresh_ts = now;
        }
    }

    if (now - g_state.last_stats_emit_ts >= TMQ_STATS_EVENT_PERIOD_S) {
        cJSON *root = cJSON_CreateObject();
        cJSON_AddNumberToObject(root, "v", 1);
        cJSON_AddNumberToObject(root, "ts", now);
        cJSON_AddStringToObject(root, "event", "plugin_stats");
        cJSON_AddNumberToObject(root, "dropped_events", (double)ring_dropped_count(&g_state.ring));
        cJSON_AddNumberToObject(root, "ring_size", (double)ring_size(&g_state.ring));
        emit_event(root);
        g_state.last_stats_emit_ts = now;
    }

    return MOSQ_ERR_SUCCESS;
}
#endif /* HAVE_MOSQUITTO */

/* ---------------------------------------------------------------------- *
 * Plugin option parsing + lifecycle
 * ---------------------------------------------------------------------- */
static void parse_options(struct mosquitto_opt *options, int option_count)
{
    strncpy(g_state.config.redis_host, "redis", sizeof(g_state.config.redis_host) - 1);
    g_state.config.redis_port = 6379;
    g_state.config.emit_batch_ms = 100;
    g_state.config.verdict_refresh_ms = 500;
    g_state.config.mode = TMQ_MODE_ENFORCE;
    g_state.config.payload_hash_enabled = 0;

    for (int i = 0; i < option_count; i++) {
        if (!options[i].key || !options[i].value) continue;
        if (strcmp(options[i].key, "redis_host") == 0) {
            strncpy(g_state.config.redis_host, options[i].value, sizeof(g_state.config.redis_host) - 1);
        } else if (strcmp(options[i].key, "redis_port") == 0) {
            g_state.config.redis_port = atoi(options[i].value);
        } else if (strcmp(options[i].key, "emit_batch_ms") == 0) {
            g_state.config.emit_batch_ms = atoi(options[i].value);
        } else if (strcmp(options[i].key, "verdict_refresh_ms") == 0) {
            g_state.config.verdict_refresh_ms = atoi(options[i].value);
        } else if (strcmp(options[i].key, "mode") == 0) {
            if (strcmp(options[i].value, "monitor") == 0) {
                g_state.config.mode = TMQ_MODE_MONITOR;
            } else if (strcmp(options[i].value, "fingerprint") == 0) {
                g_state.config.mode = TMQ_MODE_FINGERPRINT;
            } else {
                g_state.config.mode = TMQ_MODE_ENFORCE;
            }
        } else if (strcmp(options[i].key, "payload_hash") == 0) {
            g_state.config.payload_hash_enabled = (strcmp(options[i].value, "sha256") == 0);
        }
    }
}

int mosquitto_plugin_version(int supported_version_count, const int *supported_versions)
{
    (void)supported_version_count;
    (void)supported_versions;
    return 5;
}

int mosquitto_plugin_init(mosquitto_plugin_id_t *identifier, void **user_data,
                           struct mosquitto_opt *options, int option_count)
{
    (void)user_data;
    memset(&g_state, 0, sizeof(g_state));
    parse_options(options, option_count);

    ring_init(&g_state.ring);
    verdict_cache_init(&g_state.verdict_cache);
    registry_init(&g_state.registry);
    g_state.verdict_backoff_ms = TMQ_VERDICT_BACKOFF_START_MS;

    if (emitter_start(&g_state.emitter, &g_state.ring, g_state.config.redis_host,
                      g_state.config.redis_port, g_state.config.emit_batch_ms) != 0) {
        fprintf(stderr, "trustmqtt_plugin: failed to start emitter thread\n");
    }

    printf("trustmqtt_plugin: init (mode=%d, redis=%s:%d)\n",
           g_state.config.mode, g_state.config.redis_host, g_state.config.redis_port);

#ifdef HAVE_MOSQUITTO
    g_state.mosq_plugin_id = identifier;

    mosquitto_callback_register(identifier, MOSQ_EVT_CONNECT,
                                 (MOSQ_FUNC_generic_callback)handle_connect, NULL, NULL);
    mosquitto_callback_register(identifier, MOSQ_EVT_DISCONNECT,
                                 (MOSQ_FUNC_generic_callback)handle_disconnect, NULL, NULL);
    mosquitto_callback_register(identifier, MOSQ_EVT_CLIENT_OFFLINE,
                                 (MOSQ_FUNC_generic_callback)handle_client_offline, NULL, NULL);
    mosquitto_callback_register(identifier, MOSQ_EVT_MESSAGE_IN,
                                 (MOSQ_FUNC_generic_callback)handle_message_in, NULL, NULL);
    mosquitto_callback_register(identifier, MOSQ_EVT_SUBSCRIBE,
                                 (MOSQ_FUNC_generic_callback)handle_subscribe, NULL, NULL);
    mosquitto_callback_register(identifier, MOSQ_EVT_UNSUBSCRIBE,
                                 (MOSQ_FUNC_generic_callback)handle_unsubscribe, NULL, NULL);
    mosquitto_callback_register(identifier, MOSQ_EVT_BASIC_AUTH,
                                 (MOSQ_FUNC_generic_callback)handle_basic_auth, NULL, NULL);
    mosquitto_callback_register(identifier, MOSQ_EVT_ACL_CHECK,
                                 (MOSQ_FUNC_generic_callback)handle_acl_check, NULL, NULL);
    mosquitto_callback_register(identifier, MOSQ_EVT_TICK,
                                 (MOSQ_FUNC_generic_callback)handle_tick, NULL, NULL);
#else
    (void)identifier;
#endif

    return MOSQ_ERR_SUCCESS;
}

int mosquitto_plugin_cleanup(void *user_data, struct mosquitto_opt *options, int option_count)
{
    (void)user_data;
    (void)options;
    (void)option_count;

    printf("trustmqtt_plugin: cleanup\n");

#ifdef HAVE_MOSQUITTO
    if (g_state.mosq_plugin_id) {
        mosquitto_callback_unregister(g_state.mosq_plugin_id, MOSQ_EVT_CONNECT,
                                       (MOSQ_FUNC_generic_callback)handle_connect, NULL);
        mosquitto_callback_unregister(g_state.mosq_plugin_id, MOSQ_EVT_DISCONNECT,
                                       (MOSQ_FUNC_generic_callback)handle_disconnect, NULL);
        mosquitto_callback_unregister(g_state.mosq_plugin_id, MOSQ_EVT_CLIENT_OFFLINE,
                                       (MOSQ_FUNC_generic_callback)handle_client_offline, NULL);
        mosquitto_callback_unregister(g_state.mosq_plugin_id, MOSQ_EVT_MESSAGE_IN,
                                       (MOSQ_FUNC_generic_callback)handle_message_in, NULL);
        mosquitto_callback_unregister(g_state.mosq_plugin_id, MOSQ_EVT_SUBSCRIBE,
                                       (MOSQ_FUNC_generic_callback)handle_subscribe, NULL);
        mosquitto_callback_unregister(g_state.mosq_plugin_id, MOSQ_EVT_UNSUBSCRIBE,
                                       (MOSQ_FUNC_generic_callback)handle_unsubscribe, NULL);
        mosquitto_callback_unregister(g_state.mosq_plugin_id, MOSQ_EVT_BASIC_AUTH,
                                       (MOSQ_FUNC_generic_callback)handle_basic_auth, NULL);
        mosquitto_callback_unregister(g_state.mosq_plugin_id, MOSQ_EVT_ACL_CHECK,
                                       (MOSQ_FUNC_generic_callback)handle_acl_check, NULL);
        mosquitto_callback_unregister(g_state.mosq_plugin_id, MOSQ_EVT_TICK,
                                       (MOSQ_FUNC_generic_callback)handle_tick, NULL);
    }
#endif

    emitter_stop(g_state.emitter);
    if (g_state.verdict_redis_ctx) {
        redisFree(g_state.verdict_redis_ctx);
    }
    verdict_cache_destroy(&g_state.verdict_cache);
    registry_destroy(&g_state.registry);
    ring_destroy(&g_state.ring);

    return MOSQ_ERR_SUCCESS;
}
