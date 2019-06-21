/* jshint esversion: 6, node: true */

'use strict';

const {Client, Deadline, NettyChannel, SelfRefreshingChannel, Service} = require('../lib');
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

function channelProvider(cb) {
  net.createConnection({port: 8080}).setNoDelay()
    .on('error', cb)
    .once('connect', function () {
      this.removeListener('error', cb);
      cb(null, new NettyChannel(this).once('close', () => { this.end(); }));
    });
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

const deadline = Deadline.forMillis(2000);
deadline.whenExpired(() => { chan.close(); });
poll(deadline);

function poll(deadline) {
  let i = 0;
  const timer = setInterval(emit, 500);

  deadline.whenExpired(() => { clearInterval(timer); });

  function emit() {
    echoClient.emitMessage(deadline).echo('poll-' + (i++), (err) => {
      if (err) {
        console.error(err);
        return;
      }
      console.log('ok');
    });
  }
}
