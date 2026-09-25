# Order Service

The Order Service is a backend service that receives orders from the store-front and sends these orders to a RabbitMQ message queue. It enables decoupling of the order processing logic from the product service, allowing for more scalable and maintainable architecture.

## Requirements

- Node.js 24 LTS and npm, installed below
- RabbitMQ running on the same VM or local machine
- Start inside the repository's `order-service` directory. The main guide already takes you there.

## Setup Instructions

1. Update the package list and add the NodeSource repository for Node.js 24:

   ```bash
   sudo apt update
   curl -fsSL https://deb.nodesource.com/setup_24.x | sudo -E bash -
   ```

2. Install Node.js and its bundled npm:

   ```bash
   sudo apt install -y nodejs
   node --version
   npm --version
   ```

   Node.js should report `v24.x`. Use this same runtime for the Store Front. [Node.js release schedule](https://nodejs.org/en/about/previous-releases)

3. Install the versions recorded in the committed lockfile:

   ```bash
   npm ci
   ```

4. Start the service:

   ```bash
   node index.js
   ```

   Expect `Order service is running on http://localhost:3000`. Keep this terminal open; do not start a second copy from the main guide.

The service listens on port 3000 on all interfaces. On the VM, send requests to `http://localhost:3000/orders`. From your laptop, use `http://<VM-PUBLIC-IP>:3000/orders` with port 3000 allowed by the NSG. VS Code port forwarding is an optional alternative for accessing a forwarded port through your laptop's localhost.

## Testing

From another terminal, use the VS Code **REST Client** extension with `test-order-service.http`, or run:

```bash
curl -i -X POST http://localhost:3000/orders \
  -H 'Content-Type: application/json' \
  -d '{"product":"Cat Food"}'
sudo rabbitmqctl list_queues name durable messages
```

Expect HTTP 200 with `Order received` and an increased count in the durable `order_queue`. The service waits for RabbitMQ to confirm a persistent, routable message before reporting success, then closes the request's connection. Orders are accepted and queued; this lab has no consumer that fulfills them.
