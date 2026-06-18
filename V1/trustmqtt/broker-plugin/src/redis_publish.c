/*
 * Threaded Redis publisher: plugin hooks enqueue JSON strings quickly into an
 * internal queue; a background worker thread pops items and calls hiredis to
 * LPUSH them into Redis. This keeps the broker path non-blocking.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <hiredis/hiredis.h>

struct queue_node {
    char *json;
    struct queue_node *next;
};

static struct queue_node *queue_head = NULL;
static struct queue_node *queue_tail = NULL;
static pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t queue_cond = PTHREAD_COND_INITIALIZER;
static pthread_t worker_thread;
static int worker_running = 0;
static char redis_host_g[256] = "127.0.0.1";
static int redis_port_g = 6379;

static void *redis_worker(void *arg)
{
    (void)arg;
    redisContext *c = NULL;
    while (1) {
        pthread_mutex_lock(&queue_mutex);
        while (!queue_head && worker_running) {
            pthread_cond_wait(&queue_cond, &queue_mutex);
        }
        if (!worker_running && !queue_head) {
            pthread_mutex_unlock(&queue_mutex);
            break;
        }
        struct queue_node *n = queue_head;
        if (n) {
            queue_head = n->next;
            if (!queue_head) queue_tail = NULL;
        }
        pthread_mutex_unlock(&queue_mutex);

        if (!n) continue;

        /* ensure connection */
        if (!c) {
            c = redisConnect(redis_host_g, redis_port_g);
            if (!c || c->err) {
                if (c) redisFree(c);
                c = NULL;
                /* on connect failure, sleep and retry later */
                sleep(1);
                /* re-enqueue item at tail to preserve order */
                pthread_mutex_lock(&queue_mutex);
                n->next = NULL;
                if (queue_tail) queue_tail->next = n; else queue_head = n;
                queue_tail = n;
                pthread_mutex_unlock(&queue_mutex);
                continue;
            }
        }

        /* Execute LPUSH command; use %s is unsafe for strings with spaces/newlines,
         * but for now we assume plugin JSON is compact; in production use binary-safe APIs. */
        redisReply *reply = redisCommand(c, "LPUSH trustmqtt:metadata_queue %s", n->json);
        if (reply) freeReplyObject(reply);
        free(n->json);
        free(n);
    }
    if (c) redisFree(c);
    return NULL;
}

int redis_worker_init(const char *host, int port)
{
    if (!host) return -1;
    strncpy(redis_host_g, host, sizeof(redis_host_g)-1);
    redis_host_g[sizeof(redis_host_g)-1] = '\0';
    redis_port_g = port;
    worker_running = 1;
    if (pthread_create(&worker_thread, NULL, redis_worker, NULL) != 0) {
        worker_running = 0;
        return -1;
    }
    return 0;
}

void redis_worker_cleanup(void)
{
    pthread_mutex_lock(&queue_mutex);
    worker_running = 0;
    pthread_cond_signal(&queue_cond);
    pthread_mutex_unlock(&queue_mutex);
    pthread_join(worker_thread, NULL);

    /* free remaining nodes */
    while (queue_head) {
        struct queue_node *n = queue_head;
        queue_head = n->next;
        free(n->json);
        free(n);
    }
}

int enqueue_metadata_json(const char *json)
{
    if (!json) return -1;
    struct queue_node *n = malloc(sizeof(*n));
    if (!n) return -1;
    n->json = strdup(json);
    n->next = NULL;

    pthread_mutex_lock(&queue_mutex);
    if (queue_tail) queue_tail->next = n; else queue_head = n;
    queue_tail = n;
    pthread_cond_signal(&queue_cond);
    pthread_mutex_unlock(&queue_mutex);
    return 0;
}

