package org.trustmqtt.hivemq;

import com.hivemq.extension.sdk.api.ExtensionMain;
import com.hivemq.extension.sdk.api.auth.SimpleAuthenticator;
import com.hivemq.extension.sdk.api.auth.SubscriptionAuthorizer;
import com.hivemq.extension.sdk.api.auth.parameter.TopicPermission;
import com.hivemq.extension.sdk.api.events.client.ClientLifecycleEventListener;
import com.hivemq.extension.sdk.api.events.client.parameters.*;
import com.hivemq.extension.sdk.api.parameter.*;
import com.hivemq.extension.sdk.api.packets.auth.DefaultAuthorizationBehaviour;
import com.hivemq.extension.sdk.api.services.Services;
import com.hivemq.extension.sdk.api.services.builder.Builders;
import java.util.*;

/** SDK callbacks only inspect packet metadata and local state; Redis is background work. */
public final class TrustMqttExtension implements ExtensionMain {
    private BrokerRuntime runtime;
    @Override public void extensionStart(ExtensionStartInput input, ExtensionStartOutput output) {
        Configuration config = new Configuration(System.getenv());
        runtime = new BrokerRuntime(config);
        if (config.demoAuth) {
            // Explicit development-only identity policy. No username-based plant grants.
            Services.securityRegistry().setAuthenticatorProvider(provider -> (SimpleAuthenticator)(in, out) -> {
                String id = in.getClientInformation().getClientId();
                if (id.isEmpty() || id.indexOf('/') >= 0 || id.indexOf('#') >= 0 || id.indexOf('+') >= 0) {
                    out.failAuthentication(); return;
                }
                var permissions = out.getDefaultPermissions();
                permissions.setDefaultBehaviour(DefaultAuthorizationBehaviour.DENY);
                for (String topic : List.of("fleet/" + id + "/#", "tmq/quarantine/" + id + "/#")) {
                    permissions.add(Builders.topicPermission().topicFilter(topic)
                        .activity(TopicPermission.MqttActivity.ALL).type(TopicPermission.PermissionType.ALLOW).build());
                }
                out.authenticateSuccessfully();
            });
        }
        Services.eventRegistry().setClientLifecycleEventListener(provider -> new Lifecycle(runtime));
        Services.securityRegistry().setAuthorizerProvider(provider -> (SubscriptionAuthorizer)(in, out) -> {
            if (runtime.deny(in.getClientInformation().getClientId(), in.getSubscription().getTopicFilter(), false)) out.failAuthorization();
            else out.nextExtensionOrDefault();
        });
        Services.initializerRegistry().setClientInitializer((in, context) -> {
            context.addPublishInboundInterceptor((pi, po) -> {
                String id = pi.getClientInformation().getClientId();
                var p = pi.getPublishPacket();
                runtime.touch(id);
                Map<String,Object> e = runtime.event("publish", id);
                e.put("topic", p.getTopic()); e.put("qos", p.getQos().getQosNumber()); e.put("retain", p.getRetain());
                e.put("payload_len", p.getPayload().map(b -> b.remaining()).orElse(0));
                Map<String,Object> props = new LinkedHashMap<>();
                p.getContentType().ifPresent(v -> props.put("content_type", v));
                p.getMessageExpiryInterval().ifPresent(v -> props.put("message_expiry", v));
                props.put("user_prop_count", p.getUserProperties().asList().size()); e.put("props", props);
                runtime.emit(e); // Attempt telemetry, including prevented publishes.
                // A negative acknowledgement can disconnect clients (notably QoS 0).
                // Soft restrictions suppress onward delivery with a success ACK;
                // only KICK deliberately disconnects. Measure subscriber delivery.
                if (runtime.deny(id, p.getTopic(), true)) po.preventPublishDelivery();
            });
            context.addPublishOutboundInterceptor((pi, po) -> {
                if (runtime.deny(pi.getClientInformation().getClientId(), pi.getPublishPacket().getTopic(), false)) po.preventPublishDelivery();
            });
            context.addSubscribeInboundInterceptor((si, so) -> {
                String id = si.getClientInformation().getClientId(); runtime.touch(id);
                for (var subscription : si.getSubscribePacket().getSubscriptions()) {
                    Map<String,Object> e = runtime.event("subscribe", id);
                    e.put("topic", subscription.getTopicFilter()); e.put("qos", subscription.getQos().getQosNumber()); runtime.emit(e);
                }
            });
            context.addUnsubscribeInboundInterceptor((ui, uo) -> {
                String id = ui.getClientInformation().getClientId(); runtime.touch(id);
                for (String topic : ui.getUnsubscribePacket().getTopicFilters()) {
                    Map<String,Object> e = runtime.event("unsubscribe", id); e.put("topic", topic); runtime.emit(e);
                }
            });
        });
        runtime.start(Services.extensionExecutorService(), id -> Services.clientService().disconnectClient(id, true));
        System.out.println("TrustMQTT HiveMQ started: mode=" + config.mode + ", broker_id=" + config.brokerId);
    }
    @Override public void extensionStop(ExtensionStopInput input, ExtensionStopOutput output) {
        if (runtime != null) runtime.close();
    }
    static final class Lifecycle implements ClientLifecycleEventListener {
        private final BrokerRuntime r;
        private Map<String,Object> pending;
        private BrokerRuntime.Client client;
        Lifecycle(BrokerRuntime r) { this.r = r; }
        public void onMqttConnectionStart(ConnectionStartInput in) {
            var p = in.getConnectPacket();
            pending = r.event("connect", p.getClientId());
            pending.put("protocol", "mqtt");
            pending.put("keepalive", p.getKeepAlive());
            // MQTT 3 clean session maps directly. MQTT 5 also requires zero session expiry.
            pending.put("clean_session", p.getCleanStart() && p.getSessionExpiryInterval() == 0);
            p.getUserName().ifPresent(v -> pending.put("username", v));
            in.getConnectionInformation().getInetAddress().ifPresent(v -> pending.put("ip", v.getHostAddress()));
            Map<String,Object> auth = r.event("auth_observe", p.getClientId());
            if (pending.containsKey("username")) auth.put("username", pending.get("username"));
            if (pending.containsKey("ip")) auth.put("ip", pending.get("ip"));
            r.emit(auth);
        }
        public void onAuthenticationSuccessful(AuthenticationSuccessfulInput in) {
            String id = in.getClientInformation().getClientId();
            client = new BrokerRuntime.Client();
            if (pending == null) pending = r.event("connect", id);
            client.keepalive = ((Number)pending.getOrDefault("keepalive", 0)).intValue();
            r.clients.put(id, client);
            pending.put("client_id", id); pending.put("ts", BrokerRuntime.now());
            r.emit(pending); pending = null;
        }
        public void onDisconnect(DisconnectEventInput in) {
            if (client == null) return; // Failed auth is not an established connection.
            String id = in.getClientInformation().getClientId();
            r.clients.remove(id, client); // Old connection must not remove a takeover's new state.
            r.emit(r.event("disconnect", id)); client = null;
        }
    }
}
