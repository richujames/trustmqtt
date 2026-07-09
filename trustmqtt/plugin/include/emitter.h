/* emitter.h — background thread that drains the ring buffer and pipelines
 * XADD tmq:events over a single hiredis connection (spec §3.3). This is the
 * only place in the plugin that performs blocking Redis I/O for the event
 * out-path; Mosquitto callbacks never touch the network directly. */
#ifndef TMQ_EMITTER_H
#define TMQ_EMITTER_H

#include "ring.h"

typedef struct tmq_emitter tmq_emitter_t;

int emitter_start(tmq_emitter_t **out, tmq_ring_t *ring,
                   const char *redis_host, int redis_port, int batch_ms);
void emitter_stop(tmq_emitter_t *e);

#endif /* TMQ_EMITTER_H */
