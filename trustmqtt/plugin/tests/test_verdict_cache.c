#include "../include/verdict_cache.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>

static int g_kicked = 0;

static void kick_cb(const char *client_id, void *ud)
{
    (void)ud;
    if (strcmp(client_id, "dev-2") == 0) {
        g_kicked++;
    }
}

int main(void)
{
    tmq_verdict_cache_t c;
    verdict_cache_init(&c);

    tmq_verdict_entry_t out;
    assert(verdict_cache_get(&c, "unknown", &out) == 0);

    double now = 1000.0;
    verdict_cache_upsert(&c, "dev-1", TMQ_VERDICT_QUARANTINE, 0.8, now + 120.0, 2.0, now);
    assert(verdict_cache_get(&c, "dev-1", &out) == 1);
    assert(out.level == TMQ_VERDICT_QUARANTINE);
    assert(out.score > 0.79 && out.score < 0.81);

    /* Before expiry, decay is a no-op. */
    verdict_cache_decay_pass(&c, now + 10.0);
    verdict_cache_get(&c, "dev-1", &out);
    assert(out.level == TMQ_VERDICT_QUARANTINE);

    /* After expiry + 60s, level steps down by exactly one. */
    verdict_cache_decay_pass(&c, now + 120.0 + 61.0);
    verdict_cache_get(&c, "dev-1", &out);
    assert(out.level == TMQ_VERDICT_THROTTLE);

    /* Keep decaying until the entry reaches ALLOW and is evicted entirely. */
    double t = now + 120.0 + 61.0;
    int iterations = 0;
    while (verdict_cache_get(&c, "dev-1", &out) && iterations < 10) {
        t += 61.0;
        verdict_cache_decay_pass(&c, t);
        iterations++;
    }
    assert(verdict_cache_get(&c, "dev-1", &out) == 0);

    /* KICK processing demotes to QUARANTINE and fires the callback once. */
    verdict_cache_upsert(&c, "dev-2", TMQ_VERDICT_KICK, 0.95, now + 120.0, 0.0, now);
    verdict_cache_process_kicks(&c, kick_cb, NULL);
    verdict_cache_get(&c, "dev-2", &out);
    assert(out.level == TMQ_VERDICT_QUARANTINE);
    assert(g_kicked == 1);

    /* A second kick pass does nothing further (already demoted). */
    verdict_cache_process_kicks(&c, kick_cb, NULL);
    assert(g_kicked == 1);

    verdict_cache_destroy(&c);
    printf("test_verdict_cache: OK\n");
    return 0;
}
