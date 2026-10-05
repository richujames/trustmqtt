#include "trustmqtt_core.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

int main(void)
{
    tmq_config_t config;
    tmq_config_defaults(&config, "flashmq");
    assert(strcmp(config.broker, "flashmq") == 0);
    assert(strcmp(config.broker_id, "default") == 0);
    assert(strcmp(config.redis_host, "redis") == 0);
    assert(config.redis_port == 6379);
    assert(config.mode == TMQ_MODE_ENFORCE);

    assert(tmq_config_set(&config, "broker_id", "edge-west-1") == 1);
    assert(tmq_config_set(&config, "redis_host", "redis.internal") == 1);
    assert(tmq_config_set(&config, "redis_port", "6380") == 1);
    assert(tmq_config_set(&config, "mode", "monitor") == 1);
    assert(tmq_config_set(&config, "payload_hash", "sha256") == 1);
    assert(strcmp(config.broker_id, "edge-west-1") == 0);
    assert(strcmp(config.redis_host, "redis.internal") == 0);
    assert(config.redis_port == 6380);
    assert(config.mode == TMQ_MODE_MONITOR);
    assert(config.payload_hash_enabled == 1);

    assert(tmq_config_set(&config, "future_adapter_option", "x") == 0);
    assert(tmq_config_set(&config, "redis_port", "70000") == -1);
    assert(tmq_config_set(&config, "emit_batch_ms", "0") == -1);
    assert(tmq_config_set(&config, "mode", "invalid") == -1);
    assert(tmq_config_set(&config, "payload_hash", "md5") == -1);

    puts("test_core_config: OK");
    return 0;
}
