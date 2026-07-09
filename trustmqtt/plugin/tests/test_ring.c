#include "../include/ring.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static char *dup_str(const char *s)
{
    size_t n = strlen(s) + 1;
    char *out = malloc(n);
    memcpy(out, s, n);
    return out;
}

int main(void)
{
    tmq_ring_t r;
    ring_init(&r);

    assert(ring_size(&r) == 0);
    assert(ring_dropped_count(&r) == 0);

    assert(ring_push(&r, dup_str("a")) == 1);
    assert(ring_push(&r, dup_str("b")) == 1);
    assert(ring_size(&r) == 2);

    char *out[8];
    size_t n = ring_pop_batch(&r, out, 8);
    assert(n == 2);
    assert(strcmp(out[0], "a") == 0);
    assert(strcmp(out[1], "b") == 0);
    free(out[0]);
    free(out[1]);
    assert(ring_size(&r) == 0);

    for (size_t i = 0; i < TMQ_RING_SIZE; i++) {
        char buf[16];
        snprintf(buf, sizeof(buf), "%zu", i);
        assert(ring_push(&r, dup_str(buf)) == 1);
    }
    assert(ring_size(&r) == TMQ_RING_SIZE);
    assert(ring_push(&r, dup_str("overflow")) == 0);
    assert(ring_dropped_count(&r) == 1);

    static char *drain[TMQ_RING_SIZE];
    n = ring_pop_batch(&r, drain, TMQ_RING_SIZE);
    assert(n == TMQ_RING_SIZE);
    for (size_t i = 0; i < n; i++) {
        free(drain[i]);
    }

    ring_destroy(&r);
    printf("test_ring: OK\n");
    return 0;
}
