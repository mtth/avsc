/* jshint esversion: 6, node: true */

'use strict';

const {Client, Context, Service, channels} = require('../lib');
const net = require('net');

const echoService = new Service({
  protocol: 'Echo',
  messages: {
    echo: {
      request: [{name: 'message', type: 'string'}],
      response: 'string',
    },
  },
});

const upperService = new Service({
  protocol: 'Upper',
  messages: {
    upper: {
      request: [{name: 'message', type: 'string'}],
      response: 'string',
    },
  },
});

const echoClient = new Client(echoService);

const monitor = new channels.Monitor((cb) => {
  const conn = net.createConnection({port: 8080}).setNoDelay();
  const chan = new channels.NettyClientBridge(conn).channel;
  cb(null, chan, conn);
}).on('down', (conn) => { conn.destroy(); });

echoClient.channel = monitor.channel;

const ctx = new Context(2000);
client.use((call, next) => {
  console.time(call.request.message);
  next(null, (err, prev) => {
    console.timeEnd(call.request.message);
    prev(err);
  });
});

poll();

function poll() {
  let i = 0;
  const timer = setInterval(emit, 500);

  ctx.onCancel(() => {
    clearInterval(timer);
    monitor.destroy();
  });

  function emit() {
    client.emitMessage(ctx).echo('poll-' + (i++), (err, str) => {
      if (err) {
        console.error(err);
      }
    });
  }
}
