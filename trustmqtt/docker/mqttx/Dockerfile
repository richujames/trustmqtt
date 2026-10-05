FROM node:22-bookworm-slim
WORKDIR /app
COPY traffic-simulator/mqttx/package*.json ./
RUN npm ci --omit=dev --no-audit --no-fund
COPY traffic-simulator/mqttx/*.cjs ./
COPY traffic-simulator/mqttx/sensor.js ./
USER node
ENTRYPOINT ["node", "run.cjs"]
