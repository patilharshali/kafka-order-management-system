// Order-service role Kafka producer
// 

const express = require('express');
const cors = require('cors');
const { Kafka } = require('kafkajs');

const app = express();

app.use(cors());
app.use(express.json());

const kafka = new Kafka({
    clientId: 'order-producer',
    brokers: [process.env.KAFKA_BROKER || 'kafka:9092']
});
const producer = kafka.producer();
app.get('/', (req, res) => res.send('Order Service is Live!'));

app.post('/order', async (req, res) => {
   
        // We use req.body if the frontend sends data, or fallback to Laptop
        const orderData = {
            id: Date.now(),
            item: req.body.item || 'Laptop',
            userId: req.body.userId || 'user_123' // Important for SSE to find the right user!
        };

        await producer.send({
            topic: 'order-created',
            messages: [{ 
                key: orderData.userId, // partition key
                value: JSON.stringify(orderData) }],
        });

        console.log("Order produced to Kafka:", orderData);
        await producer.disconnect();
        res.status(202).json({ orderId: orderData.id });
});

const start = async () =>{
    await producer.connect();
// Explicitly bind to 0.0.0.0 for Docker stability
app.listen(5000, '0.0.0.0', () => {
    console.log('🚀 Order Service listening on port 5000 (Mapped to 5005 on Host)');
});
}
start()
