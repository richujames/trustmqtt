#include "../include/verdict_cache.h"
#include <assert.h>
#include <stdio.h>

int main(void)
{
    tmq_verdict_cache_t c;
    verdict_cache_init(&c);

    double now = 0.0;
    /* rate = 2 tokens/sec -> burst cap = 2*rate = 4 tokens, full on creation. */
    verdict_cache_upsert(&c, "dev-1", TMQ_VERDICT_THROTTLE, 0.6, now + 120.0, 2.0, now);

    int allowed = 0;
    for (int i = 0; i < 4; i++) {
        allowed += verdict_cache_try_consume_token(&c, "dev-1", now);
    }
    assert(allowed == 4);
    assert(verdict_cache_try_consume_token(&c, "dev-1", now) == 0);

    /* After 1s at rate=2/s, exactly 2 more tokens are available. */
    now += 1.0;
    assert(verdict_cache_try_consume_token(&c, "dev-1", now) == 1);
    assert(verdict_cache_try_consume_token(&c, "dev-1", now) == 1);
    assert(verdict_cache_try_consume_token(&c, "dev-1", now) == 0);

    /* Refill caps at 2*rate even after a long idle gap. */
    now += 100.0;
    allowed = 0;
    for (int i = 0; i < 10; i++) {
        allowed += verdict_cache_try_consume_token(&c, "dev-1", now);
    }
    assert(allowed == 4);

    /* Unknown client: fail-open (allow, no cached state to throttle against). */
    assert(verdict_cache_try_consume_token(&c, "ghost", now) == 1);

    verdict_cache_destroy(&c);
    printf("test_token_bucket: OK\n");
    return 0;
}
