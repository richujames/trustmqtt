#include "enforce.h"
#include <string.h>

#define TMQ_QUARANTINE_FILTER "tmq/quarantine/#"
#define TMQ_TOPIC_MAX_SEGMENTS 32

typedef struct {
    const char *ptr;
    size_t len;
} tmq_segment_t;

static size_t split_segments(const char *s, tmq_segment_t *out, size_t max_segs)
{
    size_t n = 0;
    const char *p = s;
    while (n < max_segs) {
        const char *slash = strchr(p, '/');
        out[n].ptr = p;
        out[n].len = slash ? (size_t)(slash - p) : strlen(p);
        n++;
        if (!slash) {
            break;
        }
        p = slash + 1;
    }
    return n;
}

int tmq_topic_matches(const char *filter, const char *topic)
{
    tmq_segment_t fsegs[TMQ_TOPIC_MAX_SEGMENTS];
    tmq_segment_t tsegs[TMQ_TOPIC_MAX_SEGMENTS];
    size_t fn = split_segments(filter, fsegs, TMQ_TOPIC_MAX_SEGMENTS);
    size_t tn = split_segments(topic, tsegs, TMQ_TOPIC_MAX_SEGMENTS);

    size_t i = 0;
    for (; i < fn; i++) {
        if (fsegs[i].len == 1 && fsegs[i].ptr[0] == '#') {
            /* '#' matches everything remaining, including the parent topic
             * with zero further segments (e.g. "tmq/quarantine/#" matches
             * "tmq/quarantine" itself, per MQTT semantics). */
            return 1;
        }
        if (i >= tn) {
            return 0;
        }
        int seg_ok = (fsegs[i].len == 1 && fsegs[i].ptr[0] == '+') ||
                     (fsegs[i].len == tsegs[i].len &&
                      strncmp(fsegs[i].ptr, tsegs[i].ptr, fsegs[i].len) == 0);
        if (!seg_ok) {
            return 0;
        }
    }
    return i == tn;
}

tmq_enforce_result_t tmq_enforce_check(tmq_verdict_cache_t *cache, const char *client_id,
                                        const char *topic, tmq_access_t access, double now)
{
    tmq_verdict_entry_t v;
    if (!verdict_cache_get(cache, client_id, &v) || v.level <= TMQ_VERDICT_WATCH) {
        return TMQ_ENFORCE_DEFER;
    }

    if (v.level == TMQ_VERDICT_THROTTLE) {
        if (access != TMQ_ACCESS_WRITE) {
            return TMQ_ENFORCE_DEFER;
        }
        return verdict_cache_try_consume_token(cache, client_id, now) ? TMQ_ENFORCE_DEFER : TMQ_ENFORCE_DENY;
    }

    /* QUARANTINE and KICK (KICK is about to be disconnected by the TICK
     * loop; until then it's treated at least as strictly as QUARANTINE). */
    if (access == TMQ_ACCESS_WRITE && tmq_topic_matches(TMQ_QUARANTINE_FILTER, topic)) {
        return TMQ_ENFORCE_DEFER;
    }
    return TMQ_ENFORCE_DENY;
}
