// SSE-service role - Kafka consumer

const express = require('express');
const { Kafka } = require('kafkajs');
const Redis = require('ioredis');
const cors = require('cors');
const jwt = require('jsonwebtoken');

const app = express();
app.use(cors());

/* ======================
   Environment
====================== */
const PORT = process.env.PORT || 5001;
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const REDIS_URL = process.env.REDIS_URL || 'redis://redis:6379';
const JWT_SECRET = process.env.JWT_SECRET || 'supersecret';

/* ======================
   Kafka Setup
====================== */
const kafka = new Kafka({
  clientId: 'sse-service',
  brokers: [KAFKA_BROKER]
});

const kafkaConsumer = kafka.consumer({ groupId: 'sse-service-group' });

/* ======================
   Redis Setup
====================== */
const redisPub = new Redis(REDIS_URL);
const redisSub = new Redis(REDIS_URL);

/* ======================
   SSE Connection Store
   userId -> Set<res>
====================== */
const connections = new Map();

/* ======================
   Utility: Heartbeat for slow clients
====================== */
function heartbeat(res) {
  res.write(`:\n\n`); // comment line to keep connection alive
}

/* ======================
   SSE Endpoint
====================== */
app.get('/status/:userId', async (req, res) => {
  const userId = req.params.userId;
  const token = req.query.token;

  // ----- Auth Check -----
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    if (decoded.userId !== userId) {
      return res.status(403).end();
    }
  } catch (err) {
    return res.status(401).end();
  }

  // ----- Set SSE headers -----
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');

  // ----- Register connection -----
  if (!connections.has(userId)) connections.set(userId, new Set());
  connections.get(userId).add(res);

  // Heartbeat interval
  const interval = setInterval(() => heartbeat(res), 15000);

  // ----- Disconnect -----
  req.on('close', () => {
    clearInterval(interval);
    connections.get(userId).delete(res);
    if (connections.get(userId).size === 0) connections.delete(userId);
  });

  // ----- Replay last offset if exists -----
  const lastOffset = await redisPub.get(`user:${userId}:lastOffset`);
  if (lastOffset) {
    // Optional: fetch cached events from Redis or DB
    // Example: send last event to reconnecting client
    const lastEvent = await redisPub.get(`user:${userId}:lastEvent`);
    if (lastEvent) res.write(`data: ${lastEvent}\n\n`);
  }
});

/* ======================
   Redis Subscriber → SSE
====================== */
redisSub.subscribe('order-events');
redisSub.on('message', (_, message) => {
  const payload = JSON.parse(message);
  const { userId } = payload;

  // Store last event + offset for reconnect
  if (payload.offset) {
    redisPub.set(`user:${userId}:lastOffset`, payload.offset);
  }
  redisPub.set(`user:${userId}:lastEvent`, JSON.stringify(payload));

  // Push to all connected SSE clients
  if (connections.has(userId)) {
    for (const res of connections.get(userId)) {
      // Non-blocking write
      try {
        res.write(`data: ${JSON.stringify(payload)}\n\n`);
      } catch (err) {
        console.warn('Slow client, skipping:', err.message);
      }
    }
  }
});

/* ======================
   Kafka → Redis Publisher
====================== */
async function startKafka() {
  await kafkaConsumer.connect();
  await kafkaConsumer.subscribe({ topic: 'order-updates' });

  await kafkaConsumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const payload = JSON.parse(message.value.toString());

      // Partition key is orderId for per-order ordering
      payload.partitionKey = payload.orderId || payload.userId;

      // Add Kafka offset for reconnect replay
      payload.offset = message.offset;

      // Publish to Redis for SSE fan-out
      await redisPub.publish('order-events', JSON.stringify(payload));
    }
  });

  console.log('✅ Kafka Consumer running for SSE service');
}

/* ======================
   Start Server
====================== */
app.listen(PORT, () => {
  console.log(`🚀 SSE Service running on port ${PORT}`);
  startKafka().catch(console.error);
});
