/* ring.h — fixed-size, mutex-protected ring buffer of heap-allocated JSON
 * strings. Callbacks push (non-blocking, O(1)); the emitter thread drains in
 * batches. Full ring => drop + count (fail-open, never blocks the caller). */
#ifndef TMQ_RING_H
#define TMQ_RING_H

#include <pthread.h>
#include <stddef.h>
#include "trustmqtt_plugin.h"

typedef struct {
    char *slots[TMQ_RING_SIZE];
    size_t head;   /* next write index */
    size_t tail;   /* next read index */
    size_t count;
    pthread_mutex_t lock;
    unsigned long dropped;
} tmq_ring_t;

void ring_init(tmq_ring_t *r);
void ring_destroy(tmq_ring_t *r);

/* Takes ownership of `json` (must be malloc'd). Returns 1 on success.
 * On failure (ring full) frees `json`, increments the drop counter, and
 * returns 0 — caller does not need to do anything further. */
int ring_push(tmq_ring_t *r, char *json);

/* Pops up to max_items strings into out[]. Returns the number popped.
 * Caller owns and must free() each returned string. */
size_t ring_pop_batch(tmq_ring_t *r, char **out, size_t max_items);

unsigned long ring_dropped_count(const tmq_ring_t *r);
size_t ring_size(tmq_ring_t *r);

#endif /* TMQ_RING_H */
