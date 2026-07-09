#include "ring.h"
#include <stdlib.h>

void ring_init(tmq_ring_t *r)
{
    r->head = 0;
    r->tail = 0;
    r->count = 0;
    r->dropped = 0;
    for (size_t i = 0; i < TMQ_RING_SIZE; i++) {
        r->slots[i] = NULL;
    }
    pthread_mutex_init(&r->lock, NULL);
}

void ring_destroy(tmq_ring_t *r)
{
    pthread_mutex_lock(&r->lock);
    while (r->count > 0) {
        free(r->slots[r->tail]);
        r->slots[r->tail] = NULL;
        r->tail = (r->tail + 1) % TMQ_RING_SIZE;
        r->count--;
    }
    pthread_mutex_unlock(&r->lock);
    pthread_mutex_destroy(&r->lock);
}

int ring_push(tmq_ring_t *r, char *json)
{
    pthread_mutex_lock(&r->lock);
    if (r->count >= TMQ_RING_SIZE) {
        r->dropped++;
        pthread_mutex_unlock(&r->lock);
        free(json);
        return 0;
    }
    r->slots[r->head] = json;
    r->head = (r->head + 1) % TMQ_RING_SIZE;
    r->count++;
    pthread_mutex_unlock(&r->lock);
    return 1;
}

size_t ring_pop_batch(tmq_ring_t *r, char **out, size_t max_items)
{
    size_t n = 0;
    pthread_mutex_lock(&r->lock);
    while (n < max_items && r->count > 0) {
        out[n] = r->slots[r->tail];
        r->slots[r->tail] = NULL;
        r->tail = (r->tail + 1) % TMQ_RING_SIZE;
        r->count--;
        n++;
    }
    pthread_mutex_unlock(&r->lock);
    return n;
}

unsigned long ring_dropped_count(const tmq_ring_t *r)
{
    return r->dropped;
}

size_t ring_size(tmq_ring_t *r)
{
    pthread_mutex_lock(&r->lock);
    size_t n = r->count;
    pthread_mutex_unlock(&r->lock);
    return n;
}
