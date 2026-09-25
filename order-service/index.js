// Import required modules
const express = require('express');  // Express is a minimal Node.js framework for building web applications.
const amqp = require('amqplib/callback_api');  // AMQP (Advanced Message Queuing Protocol) client library for communicating with RabbitMQ.
const cors = require('cors');  // CORS (Cross-Origin Resource Sharing) middleware for handling cross-origin requests.
require('dotenv').config(); // Load .env when present; existing environment variables take precedence.

const app = express();  // Create an Express application instance.
app.use(express.json());  // Middleware to parse incoming JSON request bodies.

// Enable CORS (Cross-Origin Resource Sharing) for all routes
// This allows your API to accept requests from different origins (e.g., your frontend).
app.use(cors());

// Configure the external broker and listening port for this deployment.
const RABBITMQ_CONNECTION_STRING = process.env.RABBITMQ_CONNECTION_STRING || 'amqp://localhost';
const PORT = process.env.PORT || 3000;

// Define a POST route for creating orders
// This route is accessed when a client (e.g., frontend) sends an order.
app.post('/orders', (req, res) => {
  const queue = 'order_queue';
  const msg = JSON.stringify(req.body);
  let connection;
  let connectionClosed = false;
  let finished = false;

  // Error events and operation callbacks may report the same failure.
  // Respond once, then close this request's connection and its channels.
  function finish(err) {
    if (finished) return;
    finished = true;

    if (err) {
      console.error('Error queuing order:', err.message);
      res.status(500).send('Error queuing order');
    } else {
      console.log('Sent order to queue:', msg);
      res.send('Order received');
    }

    if (connection && !connectionClosed) {
      try {
        connection.close((closeError) => {
          if (closeError) console.error('Error closing RabbitMQ connection:', closeError.message);
        });
      } catch (closeError) {
        // A broker failure may already have closed the connection.
        console.error('Error closing RabbitMQ connection:', closeError.message);
      }
    }
  }

  amqp.connect(RABBITMQ_CONNECTION_STRING, (err, conn) => {
    if (err) return finish(err);
    connection = conn;
    conn.on('error', finish);
    conn.on('close', () => {
      connectionClosed = true;
      finish(new Error('RabbitMQ connection closed before confirming the order'));
    });

    try {
      // A confirm channel lets RabbitMQ acknowledge receipt of the message.
      const pendingChannel = conn.createConfirmChannel((err, channel) => {
        if (finished) return;
        if (err) return finish(err);

        try {
          // Durable queues survive restarts and are supported by RabbitMQ 4.3+.
          channel.assertQueue(queue, { durable: true }, (err) => {
            if (finished) return;
            if (err) return finish(err);

            try {
              // Persistent messages survive restarts. Mandatory messages are
              // returned if unroutable. The callback waits for confirmation.
              channel.sendToQueue(queue, Buffer.from(msg), { persistent: true, mandatory: true }, finish);
            } catch (publishError) {
              finish(publishError);
            }
          });
        } catch (queueError) {
          finish(queueError);
        }
      });
      // Listen immediately, including while the channel is still opening.
      pendingChannel.on('error', finish);
      pendingChannel.on('close', () => finish(new Error('RabbitMQ channel closed before confirming the order')));
      pendingChannel.on('return', () => finish(new Error('RabbitMQ could not route the order')));
    } catch (channelError) {
      finish(channelError);
    }
  });
});

// Start the server using the configured port.
app.listen(PORT, () => {
  console.log(`Order service is running on http://localhost:${PORT}`);
});
