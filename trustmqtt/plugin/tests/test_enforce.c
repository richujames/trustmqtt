#include "../include/enforce.h"
#include <assert.h>
#include <stdio.h>

int main(void)
{
    assert(tmq_topic_matches("tmq/quarantine/#", "tmq/quarantine/dev-1/diag") == 1);
    assert(tmq_topic_matches("tmq/quarantine/#", "tmq/quarantine") == 1);
    assert(tmq_topic_matches("tmq/quarantine/#", "tmq/other/dev-1") == 0);
    assert(tmq_topic_matches("plant-a/+/temp", "plant-a/line2/temp") == 1);
    assert(tmq_topic_matches("plant-a/+/temp", "plant-a/line2/humidity") == 0);
    assert(tmq_topic_matches("a/b", "a/b/c") == 0);

    tmq_verdict_cache_t c;
    verdict_cache_init(&c);
    double now = 0.0;

    /* No cache entry => defer to normal ACLs. */
    assert(tmq_enforce_check(&c, "unknown", "any/topic", TMQ_ACCESS_WRITE, now) == TMQ_ENFORCE_DEFER);

    /* WATCH => defer (no enforcement below THROTTLE). */
    verdict_cache_upsert(&c, "dev-watch", TMQ_VERDICT_WATCH, 0.4, now + 120, 0, now);
    assert(tmq_enforce_check(&c, "dev-watch", "any/topic", TMQ_ACCESS_WRITE, now) == TMQ_ENFORCE_DEFER);

    /* THROTTLE => token bucket governs writes only; reads always defer. */
    verdict_cache_upsert(&c, "dev-throttle", TMQ_VERDICT_THROTTLE, 0.6, now + 120, 1.0, now);
    assert(tmq_enforce_check(&c, "dev-throttle", "any/topic", TMQ_ACCESS_READ, now) == TMQ_ENFORCE_DEFER);
    assert(tmq_enforce_check(&c, "dev-throttle", "any/topic", TMQ_ACCESS_WRITE, now) == TMQ_ENFORCE_DEFER);
    assert(tmq_enforce_check(&c, "dev-throttle", "any/topic", TMQ_ACCESS_WRITE, now) == TMQ_ENFORCE_DEFER);
    assert(tmq_enforce_check(&c, "dev-throttle", "any/topic", TMQ_ACCESS_WRITE, now) == TMQ_ENFORCE_DENY);

    /* QUARANTINE => only the quarantine namespace may be written; subscribe denied. */
    verdict_cache_upsert(&c, "dev-q", TMQ_VERDICT_QUARANTINE, 0.8, now + 120, 0, now);
    assert(tmq_enforce_check(&c, "dev-q", "tmq/quarantine/dev-q/diag", TMQ_ACCESS_WRITE, now) == TMQ_ENFORCE_DEFER);
    assert(tmq_enforce_check(&c, "dev-q", "plant-a/line2/temp", TMQ_ACCESS_WRITE, now) == TMQ_ENFORCE_DENY);
    assert(tmq_enforce_check(&c, "dev-q", "plant-a/#", TMQ_ACCESS_SUBSCRIBE, now) == TMQ_ENFORCE_DENY);

    verdict_cache_destroy(&c);
    printf("test_enforce: OK\n");
    return 0;
}
