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
====================== */
const connections = new Map();

/* ======================
   Auth Middleware
====================== */
async function authenticate(req, res, next) {
  const token = req.query.token;
  const userId = req.params.userId;

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    if (decoded.userId !== userId) return res.status(403).end();
    req.userId = userId;
    next();
  } catch {
    return res.status(401).end();
  }
}

/* ======================
   SSE Endpoint
====================== */
app.get('/status/:userId', authenticate, async (req, res) => {
  const userId = req.userId;

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');

  if (!connections.has(userId)) connections.set(userId, new Set());
  connections.get(userId).add(res);

  const interval = setInterval(() => res.write(':\n\n'), 15000);

  req.on('close', () => {
    clearInterval(interval);
    connections.get(userId).delete(res);
    if (connections.get(userId).size === 0) connections.delete(userId);
  });

  const lastEvent = await redisPub.get(`user:${userId}:lastEvent`);
  if (lastEvent) res.write(`data: ${lastEvent}\n\n`);
});

/* ======================
   Redis Subscriber → SSE
====================== */
(async () => {
  await redisSub.subscribe('order-events');

  redisSub.on('message', (_, message) => {
    try {
      const payload = JSON.parse(message);
      const { userId } = payload;

      redisPub.set(`user:${userId}:lastEvent`, JSON.stringify(payload));

      if (!connections.has(userId)) return;

      for (const res of connections.get(userId)) {
        const ok = res.write(`data: ${JSON.stringify(payload)}\n\n`);
        if (!ok) console.warn(`Slow client for user ${userId}`);
      }
    } catch (err) {
      console.error('Error handling Redis message:', err.message);
    }
  });
})();

/* ======================
   Kafka → Redis Publisher
====================== */
async function startKafka() {
  await kafkaConsumer.connect();
  await kafkaConsumer.subscribe({ topic: 'order-updates' });

  await kafkaConsumer.run({
    eachMessage: async ({ message }) => {
      try {
        const payload = JSON.parse(message.value.toString());
        payload.offset = message.offset;

        await redisPub.publish('order-events', JSON.stringify(payload));
      } catch (err) {
        console.error('❌ Kafka -> Redis error:', err.message);
      }
    }
  });

  console.log('✅ Kafka Consumer running for SSE service');
}

/* ======================
   Start Server
====================== */
(async () => {
  await startKafka();
  app.listen(PORT, () => {
    console.log(`🚀 SSE Service running on port ${PORT}`);
  });
})();

/* ======================
   Graceful Shutdown
====================== */
process.on('SIGINT', async () => {
  console.log('🛑 Shutting down SSE Service...');
  for (const resSet of connections.values()) {
    for (const res of resSet) res.end();
  }
  await kafkaConsumer.disconnect();
  redisPub.disconnect();
  redisSub.disconnect();
  process.exit(0);
});
