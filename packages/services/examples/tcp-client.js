/* jshint esversion: 6, node: true */

'use strict';

const {Client, Context, Service, channels} = require('../lib');
const net = require('net');

const echoService = new Service({
  protocol: 'Echo',
  messages: {
    // echo: {
    //   request: [{name: 'message', type: 'string'}],
    //   response: 'string',
    // },
    upper: {
      request: [{name: 'message', type: 'string'}],
      response: 'string',
    },
  },
});

const client = new Client(echoService);
const conn = net.createConnection({port: 8080});
client.channel = channels.netty(conn);

// setTimeout(() => { console.log('hi'); }, 10000);
const ctx = new Context(5000);
client
  .use((call, next) => {
    console.time(call.request.message);
    next(null, (err, prev) => {
      console.timeEnd(call.request.message);
      prev(err);
    });
  });

// poll();

client.emitMessage(ctx).upper('foo', (err, res) => {
  console.log(res);
  ctx.expire();
});

function poll() {
  let i = 0;
  const timer = setInterval(emit, 500);

  ctx.onCancel(() => {
    clearInterval(timer);
    conn.end();
  });

  function emit() {
    client.emitMessage(ctx).upper('poll-' + (i++), (err, str) => {
      if (err) {
        console.error(err);
      }
    });
  }
}
