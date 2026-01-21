const { Kafka } = require('kafkajs');

const kafka = new Kafka({ 
    brokers: [process.env.KAFKA_BROKER || 'kafka:9092'],
    // Added connection timeout to help with the ECONNREFUSED issues
    connectionTimeout: 10000 ,
    // Add this to both Inventory and SSE service
    allowAutoTopicCreation: true
});

const consumer = kafka.consumer({ groupId: 'inventory-worker' });
const producer = kafka.producer();

// Helper function to simulate work (3 seconds)
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const start = async () => {
    try {
        await consumer.connect();
        await producer.connect();
        await consumer.subscribe({ topic: 'order-created', fromBeginning: false });

        console.log('✅ Inventory Service is running and listening for orders...');

        await consumer.run({
            eachMessage: async ({ message }) => {
                const order = JSON.parse(message.value.toString());
                console.log('📦 Processing order:', order.id);

                // 1. Simulate "Work" correctly
                await delay(3000); 

                // 2. Send the update
                await producer.send({
                    topic: 'order-updates',
                    messages: [{
                        value: JSON.stringify({
                            userId: order.userId, // Use ID from order
                            status: `✅ Inventory Confirmed for order ${order.id}`,
                            timestamp: new Date().toISOString(),
                            orderId: order.id
                        })
                    }]
                });

                console.log(`✨ Status update sent for order ${order.id}`);
            },
        });
    } catch (error) {
        console.error('❌ Error in Inventory Service:', error);
        // Important: Exit so Docker can restart the container
        process.exit(1);
    }
}

start();