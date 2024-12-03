const { Kafka } = require('kafkajs');
const fs = require('fs');
const path = require('path');
const timestampFile = path.join(__dirname, 'lastProcessedTimestamp.txt'); // Timestamp file to track last processed time

const kafka = new Kafka({
  clientId: 'csv-producer',
  brokers: ['localhost:9092'], // Kafka broker
});

const producer = kafka.producer();

const produceMessages = async () => {
  console.log('Connecting to Kafka producer...');
  await producer.connect();
  console.log('Producer connected.');

  const filePath = path.join(__dirname, 'data.csv'); // CSV file path

  // Check if the timestamp file exists and read the last processed timestamp
  let lastProcessedTimestamp = 0;
  if (fs.existsSync(timestampFile)) {
    lastProcessedTimestamp = parseInt(fs.readFileSync(timestampFile, 'utf8'), 10);
  }

  const stats = fs.statSync(filePath); // Get the last modified time of the CSV file
  const fileLastModified = stats.mtime.getTime(); // File's last modified timestamp

  // Only proceed if the CSV file has been modified since the last processing
  if (fileLastModified <= lastProcessedTimestamp) {
    console.log('CSV file has not been modified since the last process. Skipping...');
    await producer.disconnect();
    return;
  }

  console.log('CSV file modified. Processing...');

  const csvData = fs.readFileSync(filePath, 'utf8').split('\n');

  // Skip the first line (header)
  const rows = csvData.slice(1);

  for (const row of rows) {
    if (!row.trim()) continue; // Skip empty rows

    const [Time, Customer, Facility, GrowthRate] = row.split(',');

    if (!Time || !Customer || !Facility || !GrowthRate) {
      console.error('Invalid row format:', row);
      continue; // Skip rows with missing fields
    }

    // Manually parse the date in 'DD-MM-YYYY' format
    const [day, month, year] = Time.split('-');
    
    // Ensure the date is in a valid format (YYYY-MM-DD)
    const formattedTime = new Date(`${year}-${month}-${day}`).toISOString().split('T')[0]; // Format: YYYY-MM-DD

    if (isNaN(new Date(formattedTime))) {
      console.error(`Invalid date value for Time: ${Time}`);
      continue; // Skip this row if the date is invalid
    }

    // Send each row as a JSON message
    const message = {
      Time: formattedTime,
      Customer,
      Facility,
      GrowthRate: parseFloat(GrowthRate)
    };

    try {
      await producer.send({
        topic: 'csv-topic',
        messages: [{ value: JSON.stringify(message) }],
      });
      console.log('Sent:', message);
    } catch (error) {
      console.error('Error sending message:', error);
    }
  }

  // Update the timestamp file with the current time
  fs.writeFileSync(timestampFile, fileLastModified.toString());
  console.log('CSV file processed. Timestamp updated.');

  await producer.disconnect();
  console.log('Producer disconnected.');
};

produceMessages().catch(console.error);
