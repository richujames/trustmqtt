/* flashmq_adapter.cpp — FlashMQ native adapter for the TrustMQTT C core.
 *
 * FlashMQ's public plugin ABI is C++ even though the exported entry points use
 * C linkage. This file intentionally stays thin: it translates FlashMQ SDK
 * values and maps the core's broker-neutral enforcement result.
 */
#include "trustmqtt_core.h"

#include "flashmq_plugin.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

struct ThreadData {
    std::unordered_map<std::string, int> subscription_counts;
};

tmq_core_t *g_core = nullptr;
std::mutex g_identity_mutex;
std::unordered_map<std::string, std::string> g_usernames;

void ensure_connected(const std::string &clientid, const std::string &username,
                      const char *address)
{
    bool is_new = false;
    {
        std::lock_guard<std::mutex> guard(g_identity_mutex);
        auto [entry, inserted] = g_usernames.emplace(clientid, username);
        if (!inserted) entry->second = username;
        is_new = inserted;
    }
    if (!is_new) return;
    tmq_connect_event_t event = {
        clientid.c_str(),
        username.empty() ? nullptr : username.c_str(),
        address,
        "mqtt",
        0,
        0,
        -1,
    };
    tmq_core_on_connect(g_core, &event);
}

std::string option_name(const std::string &key)
{
    static const std::string prefixes[] = {"flashmq_plugin_opt_", "plugin_opt_"};
    for (const auto &prefix : prefixes) {
        if (key.rfind(prefix, 0) == 0) return key.substr(prefix.size());
    }
    return key;
}

void kick_client(const char *client_id, void *)
{
    std::string username;
    {
        std::lock_guard<std::mutex> guard(g_identity_mutex);
        auto found = g_usernames.find(client_id);
        if (found == g_usernames.end()) return;
        username = found->second;
    }
    std::weak_ptr<Session> session;
    flashmq_get_session_pointer(client_id, username, session);
    if (!session.expired()) {
        flashmq_plugin_remove_client_v4(session, false, ServerDisconnectReasons::NotAuthorized);
    }
}

uint32_t remaining_expiry_seconds(
    const std::optional<std::chrono::time_point<std::chrono::steady_clock>> &expires_at)
{
    if (!expires_at) return 0;
    auto remaining = std::chrono::duration_cast<std::chrono::seconds>(
        *expires_at - std::chrono::steady_clock::now()).count();
    if (remaining <= 0) return 0;
    return static_cast<uint32_t>(std::min<long long>(
        remaining, std::numeric_limits<uint32_t>::max()));
}

} // namespace

extern "C" {

int flashmq_plugin_version()
{
    return FLASHMQ_PLUGIN_VERSION;
}

void flashmq_plugin_main_init(std::unordered_map<std::string, std::string> &plugin_opts)
{
    tmq_config_t config;
    tmq_config_defaults(&config, "flashmq");
    for (const auto &[raw_key, value] : plugin_opts) {
        std::string key = option_name(raw_key);
        int result = tmq_config_set(&config, key.c_str(), value.c_str());
        if (result < 0) {
            throw std::runtime_error("invalid TrustMQTT option " + key + "=" + value);
        }
    }
    g_core = tmq_core_create(&config, kick_client, nullptr);
    if (!g_core) {
        throw std::runtime_error("TrustMQTT core initialization failed");
    }
}

void flashmq_plugin_main_deinit(std::unordered_map<std::string, std::string> &)
{
    tmq_core_destroy(g_core);
    g_core = nullptr;
    std::lock_guard<std::mutex> guard(g_identity_mutex);
    g_usernames.clear();
}

void flashmq_plugin_allocate_thread_memory(
    void **thread_data, std::unordered_map<std::string, std::string> &)
{
    *thread_data = new ThreadData();
}

void flashmq_plugin_deallocate_thread_memory(
    void *thread_data, std::unordered_map<std::string, std::string> &)
{
    delete static_cast<ThreadData *>(thread_data);
}

void flashmq_plugin_init(void *, std::unordered_map<std::string, std::string> &, bool)
{
}

void flashmq_plugin_deinit(void *, std::unordered_map<std::string, std::string> &, bool)
{
}

void flashmq_plugin_periodic_event(void *)
{
    tmq_core_poll_actions(g_core);
}

AuthResult flashmq_plugin_login_check(
    void *, const std::string &clientid, const std::string &username,
    const std::string &, const std::vector<std::pair<std::string, std::string>> *,
    const std::weak_ptr<Client> &client)
{
    std::string address;
    flashmq_get_client_address_v4(client, &address, nullptr, nullptr);
    tmq_core_on_auth_observe(g_core, clientid.c_str(), username.c_str(),
                             address.empty() ? nullptr : address.c_str());
    ensure_connected(clientid, username, address.empty() ? nullptr : address.c_str());
    /* TrustMQTT observes authentication. Configure FlashMQ's password/ACL
     * files as the primary auth layer; they take precedence over this plugin. */
    return AuthResult::success;
}

void flashmq_plugin_client_disconnected(void *thread_data, const std::string &clientid)
{
    tmq_core_on_disconnect(g_core, clientid.c_str(), 0, 0, 0);
    if (thread_data) {
        static_cast<ThreadData *>(thread_data)->subscription_counts.erase(clientid);
    }
    std::lock_guard<std::mutex> guard(g_identity_mutex);
    g_usernames.erase(clientid);
}

void flashmq_plugin_on_unsubscribe(
    void *thread_data, const std::weak_ptr<Session> &, const std::string &clientid,
    const std::string &, const std::string &topic, const std::vector<std::string> &,
    const std::string &, const std::vector<std::pair<std::string, std::string>> *)
{
    int count = -1;
    if (thread_data) {
        auto &counts = static_cast<ThreadData *>(thread_data)->subscription_counts;
        int &current = counts[clientid];
        if (current > 0) current--;
        count = current;
    }
    tmq_core_on_unsubscribe(g_core, clientid.c_str(), topic.c_str(), -1, count);
}

AuthResult flashmq_plugin_acl_check(
    void *thread_data, const AclAccess access, const std::string &clientid,
    const std::string &username, const std::string &topic, const std::vector<std::string> &,
    const std::string &, std::string_view payload, const uint8_t qos,
    const bool retain, const std::optional<std::string> &,
    const std::optional<std::string> &, const std::optional<std::string> &content_type,
    const std::optional<std::chrono::time_point<std::chrono::steady_clock>> expires_at,
    const std::vector<std::pair<std::string, std::string>> *user_properties)
{
    /* FlashMQ does not invoke login_check for allowed anonymous clients. The
     * first ACL callback therefore doubles as their connection observation. */
    ensure_connected(clientid, username, nullptr);
    tmq_access_t normalized_access = TMQ_ACCESS_READ;
    if (access == AclAccess::write) normalized_access = TMQ_ACCESS_WRITE;
    if (access == AclAccess::subscribe) normalized_access = TMQ_ACCESS_SUBSCRIBE;

    tmq_enforce_result_t decision = tmq_core_check_access(
        g_core, clientid.c_str(), topic.c_str(), normalized_access);
    if (decision == TMQ_ENFORCE_DENY) return AuthResult::acl_denied;

    if (access == AclAccess::write) {
        tmq_publish_event_t event = {
            clientid.c_str(),
            topic.c_str(),
            payload.data(),
            static_cast<uint32_t>(std::min<size_t>(payload.size(),
                                                   std::numeric_limits<uint32_t>::max())),
            qos,
            retain ? 1 : 0,
            content_type ? content_type->c_str() : nullptr,
            remaining_expiry_seconds(expires_at),
            user_properties ? static_cast<int>(user_properties->size()) : 0,
        };
        tmq_core_on_publish(g_core, &event);
    } else if (access == AclAccess::subscribe) {
        int count = -1;
        if (thread_data) {
            auto &counts = static_cast<ThreadData *>(thread_data)->subscription_counts;
            count = ++counts[clientid];
        }
        tmq_core_on_subscribe(g_core, clientid.c_str(), topic.c_str(), qos, count);
    }
    return AuthResult::success;
}

} // extern "C"
