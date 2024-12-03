const { Kafka } = require('kafkajs');
const mysql = require('mysql2');
require('dotenv').config();  // Ensure this is at the top of your file

// Kafka configuration
const kafka = new Kafka({
  clientId: 'csv-consumer',
  brokers: ['localhost:9092'], // Kafka broker
});

// MySQL configuration from environment variables
const connection = mysql.createConnection({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});

const consumer = kafka.consumer({ groupId: 'csv-group' });

// Function to insert data into MySQL
const saveToDatabase = async (data) => {
  return new Promise((resolve, reject) => {
    const { Time, Customer, Facility, GrowthRate } = data;

    // SQL query to insert data
    const query = 'INSERT INTO kafkaTable (Time, Customer, Facility, GrowthRate) VALUES (?, ?, ?, ?)';
    
    connection.query(query, [Time, Customer, Facility, GrowthRate], (err, result) => {
      if (err) {
        console.error('Error saving data to database:', err);
        reject(err);
      } else {
        console.log('Data saved successfully in the database');
        resolve(result);
      }
    });
  });
};

// Function to handle graceful shutdown
const gracefulShutdown = async () => {
  console.log('Disconnecting from Kafka consumer...');
  await consumer.disconnect();
  console.log('Disconnected from Kafka consumer.');
  connection.end();
  console.log('MySQL connection closed.');
  process.exit(0); // Ensure the process exits
};

// Main consumer function
const runConsumer = async () => {
  console.log('Connecting to Kafka consumer...');
  await consumer.connect();
  console.log('Connected to Kafka consumer.');

  await consumer.subscribe({ topic: 'csv-topic', fromBeginning: true });
  console.log('Subscribed to Kafka topic.');

  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const parsedMessage = JSON.parse(message.value.toString());
        console.log('Received message:', parsedMessage);

        // Save to database
        await saveToDatabase(parsedMessage);
      } catch (err) {
        console.error('Error processing message:', err);
      }
    },
  });
};

// Graceful shutdown when process is terminated
process.on('SIGINT', gracefulShutdown);
process.on('SIGTERM', gracefulShutdown);

// Run the consumer
runConsumer().catch(console.error);
