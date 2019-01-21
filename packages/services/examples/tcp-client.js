/* jshint esversion: 6, node: true */

'use strict';

const {Client, Context, Router, Service, netty} = require('../lib');
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

const echoClient = new Client(echoService)
  .use((call, next) => {
    console.time(call.request.message);
    next(null, (err, prev) => {
      console.timeEnd(call.request.message);
      prev(err);
    });
  });

function newRouter(cb) {
  const conn = net.createConnection({port: 8080}).setNoDelay();
  netty.router(conn, (err, router) => {
    if (err) {
      cb(err);
      return;
    }
    cb(null, router, conn);
  });
}

Router.pooling(newRouter, (err, router) => {
  if (err) {
    console.error(err);
    return;
  }
  router.on('down', (conn) => { conn.destroy(); });
  echoClient.channel = router.channel;

  const ctx = new Context(2000);
  ctx.onCancel(() => { router.destroy(); });
  poll(ctx);
});


function poll(ctx) {
  let i = 0;
  const timer = setInterval(emit, 500);

  ctx.onCancel(() => { clearInterval(timer); });

  function emit() {
    echoClient.emitMessage(ctx).echo('poll-' + (i++), (err, str) => {
      if (err) {
        console.error(err);
      }
      console.log('ok');
    });
  }
}
