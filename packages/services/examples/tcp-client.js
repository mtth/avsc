/* jshint esversion: 6, node: true */

'use strict';

const {Client, NettyChannel, SelfRefreshingChannel, Service, Trace} = require('../lib');
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

function channelProvider(cb) {
  net.createConnection({port: 8080}).setNoDelay()
    .on('error', cb)
    .once('connect', function () {
      this.removeListener('error', cb);
      cb(null, new NettyChannel(this).once('close', () => { this.end(); }));
    })
}

const chan = new SelfRefreshingChannel(channelProvider)
  .on('error', (err) => { console.error(`channel error: ${err}`); });

const echoClient = new Client(echoService)
  .channel(chan)
  .use((wreq, wres, next) => {
    console.time(wreq.request.message);
    next(null, (err, prev) => {
      console.timeEnd(wreq.request.message);
      prev(err);
    });
  });

const trace = new Trace(2000);
trace.onceInactive(() => { chan.close(); });
poll(trace);

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
