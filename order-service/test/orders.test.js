const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const { readFileSync } = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

// Control broker callbacks/events independently to reproduce their ordering.
// Run the actual entrypoint without opening a port or requiring a live broker.
function request(env = {}) {
  const state = { replies: [], published: [], logs: [], closes: 0 };
  const connection = new EventEmitter();
  const channel = new EventEmitter();
  const app = {
    use() {},
    post(route, handler) {
      assert.equal(route, '/orders');
      state.handler = handler;
    },
    listen(port) { state.port = port; }
  };
  const express = () => app;
  express.json = () => () => {};
  const amqp = { connect(url, callback) { state.url = url; state.connect = callback; } };
  connection.createConfirmChannel = (callback) => { state.open = callback; return channel; };
  connection.close = (callback) => {
    state.closes++;
    channel.emit('close');
    connection.emit('close');
    callback();
  };
  channel.assertQueue = (name, options, callback) => {
    assert.equal(name, 'order_queue');
    assert.equal(options.durable, true);
    state.declare = callback;
  };
  channel.sendToQueue = (name, message, options, callback) => {
    state.published.push({ name, body: JSON.parse(message.toString()), options });
    state.confirm = callback;
    return false; // Backpressure is not a broker acknowledgement.
  };
  vm.runInNewContext(readFileSync(path.join(__dirname, '../index.js'), 'utf8'), {
    require(name) {
      if (name === 'express') return express;
      if (name === 'cors') return () => () => {};
      if (name === 'dotenv') return { config() {} };
      if (name === 'amqplib/callback_api') return amqp;
      throw new Error(`Unexpected import: ${name}`);
    },
    process: { env },
    Buffer,
    console: {
      log: (...args) => state.logs.push(args),
      error() {}
    }
  });
  const body = { product: { id: 1, name: 'Dog Food', price: 19.99 }, quantity: 2, totalPrice: 39.98 };
  const response = {
    code: 200,
    status(code) { this.code = code; return this; },
    send(text) { state.replies.push({ code: this.code, text }); }
  };
  state.handler({ body }, response);
  return Object.assign(state, {
    connection, channel, body,
    ready() {
      state.connect(null, connection);
      state.open(null, channel);
      state.declare(null);
    }
  });
}

function failed(state) {
  assert.deepEqual(state.replies, [{ code: 500, text: 'Error queuing order' }]);
  assert.equal(state.logs.length, 0, 'failed orders must not log success');
}

test('waits for declaration and broker confirmation; sends persistent, mandatory payload', () => {
  const state = request();
  state.connect(null, state.connection);
  state.open(null, state.channel);
  assert.equal(state.published.length, 0);
  assert.equal(state.replies.length, 0);
  state.declare(null);
  assert.equal(state.replies.length, 0);
  assert.equal(state.closes, 0);
  assert.equal(state.logs.length, 0);
  assert.equal(state.published[0].name, 'order_queue');
  assert.deepEqual(state.published[0].body, state.body);
  assert.equal(state.published[0].options.persistent, true);
  assert.equal(state.published[0].options.mandatory, true);
  state.confirm(null);
  assert.deepEqual(state.replies, [{ code: 200, text: 'Order received' }]);
  assert.equal(state.logs.length, 1);
  assert.equal(state.closes, 1);
  state.connection.emit('error', new Error('late error'));
  assert.equal(state.replies.length, 1);
});

test('unavailable broker returns failure', () => {
  const state = request();
  state.connect(new Error('ECONNREFUSED'));
  failed(state);
  assert.equal(state.closes, 0);
});

test('channel creation failure closes the connection', () => {
  const state = request();
  state.connect(null, state.connection);
  state.open(new Error('channel limit'));
  failed(state);
  assert.equal(state.closes, 1);
});

test('queue rejection and accompanying error event produce one failure and no publish', () => {
  const state = request();
  state.connect(null, state.connection);
  state.open(null, state.channel);
  state.declare(new Error('PRECONDITION_FAILED'));
  state.channel.emit('error', new Error('PRECONDITION_FAILED'));
  state.declare(null); // A late operation completion cannot resume the request.
  failed(state);
  assert.equal(state.published.length, 0);
  assert.equal(state.closes, 1);
});

test('broker nack returns failure and releases resources', () => {
  const state = request();
  state.ready();
  state.confirm(new Error('message nacked'));
  failed(state);
  assert.equal(state.closes, 1);
});

test('unroutable message followed by broker ack cannot report success', () => {
  const state = request();
  state.ready();
  state.channel.emit('return', { fields: { replyText: 'NO_ROUTE' } });
  state.confirm(null);
  failed(state);
  assert.equal(state.closes, 1);
});

for (const target of ['connection', 'channel']) {
  for (const event of ['error', 'close']) {
    test(`${target} ${event} during publishing returns one failure`, () => {
      const state = request();
      state.ready();
      state[target].emit(event, new Error('broker interrupted'));
      state.confirm(new Error('channel closed'));
      failed(state);
      assert.equal(state.closes, target === 'connection' && event === 'close' ? 0 : 1);
    });
  }
}

for (const operation of ['createConfirmChannel', 'assertQueue', 'sendToQueue']) {
  test(`synchronous ${operation} failure is handled`, () => {
    const state = request();
    const target = operation === 'createConfirmChannel' ? state.connection : state.channel;
    target[operation] = () => { throw new Error('channel already closed'); };
    state.connect(null, state.connection);
    if (operation !== 'createConfirmChannel') state.open(null, state.channel);
    if (operation === 'sendToQueue') state.declare(null);
    failed(state);
    assert.equal(state.closes, 1);
  });
}

for (const event of ['error', 'close']) {
  test(`channel ${event} while opening is handled before the creation callback`, () => {
    const state = request();
    state.connect(null, state.connection);
    state.channel.emit(event, new Error('channel setup interrupted'));
    state.open(new Error('channel closed'));
    failed(state);
    assert.equal(state.closes, 1);
    assert.equal(state.published.length, 0);
  });
}


test('uses local defaults when deployment configuration is absent', () => {
  const state = request();
  assert.equal(state.url, 'amqp://localhost');
  assert.equal(state.port, 3000);
});

test('uses the configured external broker and listening port', () => {
  const env = {
    RABBITMQ_CONNECTION_STRING: 'amqp://labuser:example@10.0.0.4:5672/',
    PORT: '3100'
  };
  const state = request(env);
  assert.equal(state.url, env.RABBITMQ_CONNECTION_STRING);
  assert.equal(state.port, env.PORT);
  state.ready();
  state.confirm(null);
  assert.deepEqual(state.replies, [{ code: 200, text: 'Order received' }]);
  assert.equal(state.closes, 1);
});
