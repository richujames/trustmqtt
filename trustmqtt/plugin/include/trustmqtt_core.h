/* trustmqtt_core.h — broker-neutral TrustMQTT runtime.
 *
 * Broker adapters translate their native callback data into these normalized
 * calls. The core owns event serialization, the non-blocking Redis emitter,
 * client bookkeeping, verdict refresh, and enforcement decisions. No broker
 * SDK type is exposed here, so the same C code can be linked into Mosquitto,
 * FlashMQ, or an out-of-process adapter.
 */
#ifndef TRUSTMQTT_CORE_H
#define TRUSTMQTT_CORE_H

#include <stddef.h>
#include <stdint.h>

#include "enforce.h"
#include "trustmqtt_plugin.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TMQ_CORE_ABI_VERSION 1

typedef struct tmq_core tmq_core_t;

typedef void (*tmq_core_kick_cb_t)(const char *client_id, void *userdata);

typedef struct {
    const char *client_id;
    const char *username;
    const char *ip;
    const char *protocol;
    int clean_session;
    int has_clean_session;
    int keepalive;
} tmq_connect_event_t;

typedef struct {
    const char *client_id;
    const char *topic;
    const void *payload;
    uint32_t payload_len;
    int qos;
    int retain;
    const char *content_type;
    uint32_t message_expiry;
    int user_prop_count;
} tmq_publish_event_t;

/* Defaults preserve the existing deployment. adapter_name is emitted as the
 * event's broker field and must be a stable lower-case implementation name. */
void tmq_config_defaults(tmq_config_t *config, const char *adapter_name);

/* Apply one broker-neutral plugin option. Returns 1 when recognized, 0 when
 * unknown, and -1 when the value is invalid. */
int tmq_config_set(tmq_config_t *config, const char *key, const char *value);

/* Starts the emitter and maintenance threads. Returns NULL on allocation or
 * initialization failure. Network failure is not initialization failure: the
 * core remains fail-open and reconnects in the background. */
tmq_core_t *tmq_core_create(const tmq_config_t *config,
                            tmq_core_kick_cb_t kick_cb,
                            void *kick_userdata);
void tmq_core_destroy(tmq_core_t *core);

tmq_mode_t tmq_core_mode(const tmq_core_t *core);

void tmq_core_on_connect(tmq_core_t *core, const tmq_connect_event_t *event);
void tmq_core_on_disconnect(tmq_core_t *core, const char *client_id,
                            int reason, int has_reason, int offline);
void tmq_core_on_publish(tmq_core_t *core, const tmq_publish_event_t *event);
void tmq_core_on_subscribe(tmq_core_t *core, const char *client_id,
                           const char *topic, int qos, int sub_count);
void tmq_core_on_unsubscribe(tmq_core_t *core, const char *client_id,
                             const char *topic, int qos, int sub_count);
void tmq_core_on_auth_observe(tmq_core_t *core, const char *client_id,
                              const char *username, const char *ip);

/* Returns the real TrustMQTT decision. Adapters map DEFER to their broker's
 * normal-ACL continuation result. Monitor mode logs a would-deny and returns
 * DEFER; fingerprint mode always returns DEFER. */
tmq_enforce_result_t tmq_core_check_access(tmq_core_t *core,
                                            const char *client_id,
                                            const char *topic,
                                            tmq_access_t access);

/* Runs network-backed maintenance. Normal adapters must not call this from a
 * broker event loop: the core's background thread already owns this work.
 * It remains public for deterministic integration tests. */
void tmq_core_maintenance(tmq_core_t *core);

/* Runs pending broker actions without network I/O. Call this from the
 * broker's native periodic/tick hook so broker APIs execute on a broker-owned
 * thread. Concurrent calls are safe; each pending action is claimed once. */
void tmq_core_poll_actions(tmq_core_t *core);

#ifdef __cplusplus
}
#endif

#endif /* TRUSTMQTT_CORE_H */
