const express = require('express');
const { Kafka } = require('kafkajs');
const cors = require('cors');

const app = express();
app.use(cors());

const kafka = new Kafka({
  clientId: 'sse-consumer',
  brokers: [process.env.KAFKA_BROKER || 'kafka:9092'],
  allowAutoTopicCreation: true
});

app.get('/status/:userId', async (req, res) => {
  const currentUserId = req.params.userId;

  // Set headers for SSE
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');

  // Unique group ID for every tab/user session
  const consumer = kafka.consumer({ 
    groupId: `sse-group-${currentUserId}-${Date.now()}` 
  });

  // --- Connection Logic with Retry ---
  let connected = false;
  let attempts = 0;

  while (!connected && attempts < 5) {
    try {
      await consumer.connect();
      console.log(`✅ Kafka Consumer connected for user: ${currentUserId}`);
      connected = true;
    } catch (e) {
      attempts++;
      console.error(`❌ Connection failed for ${currentUserId} (Attempt ${attempts}/5). Retrying...`);
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
  }

  if (!connected) {
    res.write('data: {"error": "Could not connect to event stream"}\n\n');
    return res.end();
  }

  // --- Main Consumer Logic ---
  try {
    await consumer.subscribe({ topic: 'order-updates', fromBeginning: false });

    await consumer.run({
      eachMessage: async ({ message }) => {
        try {
          const stringValue = message.value.toString();
          const payload = JSON.parse(stringValue);

          // Defensive Check: Ensure payload exists and has a userId
          if (payload && payload.userId) {
            if (payload.userId === currentUserId) {
              console.log(`🎯 Match found! Sending update to ${currentUserId}`);
              res.write(`data: ${JSON.stringify(payload)}\n\n`);
            } else {
              // Log that we are ignoring a message intended for someone else
              console.log(`⏭️ Ignoring message for ${payload.userId} (Current listener: ${currentUserId})`);
            }
          } else {
            console.warn("⚠️ Received message with missing userId field:", payload);
          }
        } catch (err) {
          // This inner catch ensures one malformed message doesn't crash the consumer
          console.error("❌ Error processing a specific message:", err.message);
        }
      }
    });
  } catch (err) {
    console.error("❌ Kafka Subscription/Run Error:", err);
  }

  // Handle client disconnection (closing tab)
  req.on('close', async () => {
    console.log(`👋 Client ${currentUserId} closed connection. Disconnecting consumer...`);
    try {
      await consumer.disconnect();
    } catch (err) {
      console.error("Error during consumer disconnect:", err);
    }
  });
});

app.listen(5001, '0.0.0.0', () => {
  console.log('🚀 SSE Service live on port 5001');
});