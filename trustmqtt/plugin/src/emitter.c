#include "emitter.h"
#include <hiredis/hiredis.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define TMQ_EMITTER_DRAIN_BATCH 512
#define TMQ_BACKOFF_START_MS 250
#define TMQ_BACKOFF_CAP_MS 5000

struct tmq_emitter {
    tmq_ring_t *ring;
    char redis_host[256];
    int redis_port;
    int batch_ms;
    pthread_t thread;
    volatile int running;
};

static redisContext *try_connect(const char *host, int port)
{
    struct timeval tv = {1, 0};
    redisContext *ctx = redisConnectWithTimeout(host, port, tv);
    if (!ctx || ctx->err) {
        if (ctx) {
            redisFree(ctx);
        }
        return NULL;
    }
    return ctx;
}

static void *emitter_loop(void *arg)
{
    tmq_emitter_t *e = (tmq_emitter_t *)arg;
    redisContext *ctx = NULL;
    long backoff_ms = TMQ_BACKOFF_START_MS;
    char *batch[TMQ_EMITTER_DRAIN_BATCH];

    while (e->running) {
        usleep((useconds_t)e->batch_ms * 1000);

        size_t n;
        while ((n = ring_pop_batch(e->ring, batch, TMQ_EMITTER_DRAIN_BATCH)) > 0) {
            if (!ctx) {
                ctx = try_connect(e->redis_host, e->redis_port);
            }
            if (!ctx) {
                /* Redis unreachable: fail-open by dropping this drained
                 * batch (already counted separately via ring_dropped_count
                 * for ring-full drops; connection-loss drops are an
                 * accepted trade-off of the fail-open posture). */
                for (size_t i = 0; i < n; i++) {
                    free(batch[i]);
                }
                usleep((useconds_t)backoff_ms * 1000);
                backoff_ms = backoff_ms * 2 > TMQ_BACKOFF_CAP_MS ? TMQ_BACKOFF_CAP_MS : backoff_ms * 2;
                break;
            }
            backoff_ms = TMQ_BACKOFF_START_MS;

            for (size_t i = 0; i < n; i++) {
                redisAppendCommand(ctx, "XADD tmq:events MAXLEN ~ 1000000 * v %s", batch[i]);
            }
            int connection_broken = 0;
            for (size_t i = 0; i < n; i++) {
                redisReply *reply = NULL;
                if (redisGetReply(ctx, (void **)&reply) != REDIS_OK) {
                    connection_broken = 1;
                } else if (reply) {
                    freeReplyObject(reply);
                }
                free(batch[i]);
            }
            if (connection_broken) {
                redisFree(ctx);
                ctx = NULL;
            }
        }
    }

    if (ctx) {
        redisFree(ctx);
    }
    return NULL;
}

int emitter_start(tmq_emitter_t **out, tmq_ring_t *ring,
                   const char *redis_host, int redis_port, int batch_ms)
{
    tmq_emitter_t *e = calloc(1, sizeof(tmq_emitter_t));
    if (!e) {
        return -1;
    }
    e->ring = ring;
    strncpy(e->redis_host, redis_host, sizeof(e->redis_host) - 1);
    e->redis_port = redis_port;
    e->batch_ms = batch_ms > 0 ? batch_ms : 100;
    e->running = 1;

    if (pthread_create(&e->thread, NULL, emitter_loop, e) != 0) {
        free(e);
        return -1;
    }
    *out = e;
    return 0;
}

void emitter_stop(tmq_emitter_t *e)
{
    if (!e) {
        return;
    }
    e->running = 0;
    pthread_join(e->thread, NULL);
    free(e);
}
