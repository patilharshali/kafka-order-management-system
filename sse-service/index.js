const express = require('express');
const { Kafka } = require('kafkajs');
const Redis = require('ioredis');
const cors = require('cors');

const app = express();
app.use(cors());

/* ======================
   Kafka Setup
====================== */
const kafka = new Kafka({
  clientId: 'sse-service',
  brokers: [process.env.KAFKA_BROKER || 'kafka:9092']
});

const kafkaConsumer = kafka.consumer({
  groupId: 'sse-service-group'
});

/* ======================
   Redis Setup
====================== */
const redisPub = new Redis(process.env.REDIS_URL || 'redis://redis:6379');
const redisSub = new Redis(process.env.REDIS_URL || 'redis://redis:6379');

/* ======================
   SSE Connection Store
====================== */
// userId -> Set<res>
const connections = new Map();

/* ======================
   SSE Endpoint
====================== */
app.get('/status/:userId', (req, res) => {
  const userId = req.params.userId;

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');

  if (!connections.has(userId)) {
    connections.set(userId, new Set());
  }

  connections.get(userId).add(res);

  req.on('close', () => {
    connections.get(userId).delete(res);
    if (connections.get(userId).size === 0) {
      connections.delete(userId);
    }
  });
});

/* ======================
   Redis Subscriber → SSE
====================== */
redisSub.subscribe('order-events');

redisSub.on('message', (channel, message) => {
  try {
    const payload = JSON.parse(message);
    const { userId } = payload;
    
    if (userId && connections.has(userId)) {
      connections.get(userId).forEach(res => {
        res.write(`data: ${JSON.stringify(payload)}\n\n`);
      });
    }
  } catch (err) {
    console.error("❌ Redis message parse error:", err);
  }
});

/* ======================
   Kafka → Redis
====================== */
async function startKafka() {
  await kafkaConsumer.connect();
  await kafkaConsumer.subscribe({ topic: 'order-updates' });

  await kafkaConsumer.run({
    eachMessage: async ({ message }) => {
      const payload = JSON.parse(message.value.toString());
      redisPub.publish('order-events', JSON.stringify(payload));
    }
  });
}

/* ======================
   Server Bootstrap
====================== */
app.listen(5001, () => {
  console.log('🚀 SSE Service running on port 5001');
  startKafka().catch(console.error);
});
