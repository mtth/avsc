/* jshint esversion: 6, node: true */

'use strict';

const {Client, Router, Service, Trace, netty} = require('../lib');
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
  .use((wreq, wres, next) => {
    console.time(wreq.request.message);
    next(null, (err, prev) => {
      console.timeEnd(wreq.request.message);
      prev(err);
    });
  });

function routerProvider(cb) {
  const conn = net.createConnection({port: 8080}).setNoDelay();
  netty.router(conn, (err, router) => {
    if (err) {
      cb(err);
      return;
    }
    router.once('close', () => { conn.end(); });
    cb(null, router, conn);
  });
}

Router.selfRefreshing(routerProvider, (err, router) => {
  if (err) {
    console.error(err);
    return;
  }
  router.on('error', (err) => { console.error(`router error: ${err}`); });
  echoClient.channel = router.channel;

  const trace = new Trace(2000);
  trace.onceInactive(() => { router.close(); });
  poll(trace);
});


function poll(trace) {
  let i = 0;
  const timer = setInterval(emit, 500);

  trace.onceInactive(() => { clearInterval(timer); });

  function emit() {
    echoClient.emitMessage(trace).echo('poll-' + (i++), (err, str) => {
      if (err) {
        console.error(err);
      }
      console.log('ok');
    });
  }
}
