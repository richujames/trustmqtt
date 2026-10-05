# TrustMQTT Grafana image.
#
# The redis-datasource plugin is installed at BUILD time rather than via the
# GF_INSTALL_PLUGINS environment variable, because that variable makes the
# entrypoint contact grafana.com on every single container start — even when
# the plugin is already present in /var/lib/grafana/plugins. Any DNS blip or
# registry hiccup then exits the container with code 1, so `docker compose
# restart grafana` (or a recreate) could only succeed while online. Observed
# failures that motivated this:
#   Error: ✗ failed to download plugin archive: ... EOF
#   Error: ✗ Get "https://grafana.com/api/plugins/redis-datasource/versions":
#          dial tcp: lookup grafana.com ...: no such host
# Baking it in makes the network a build-time dependency only; runtime start
# is fully offline-safe and deterministic.
FROM grafana/grafana:10.4.2

USER root
RUN grafana cli --pluginsDir /var/lib/grafana/plugins plugins install redis-datasource \
    && chown -R grafana:root /var/lib/grafana/plugins
USER grafana
