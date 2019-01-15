/* jshint esversion: 6, node: true */

'use strict';

const {Server, Service, channels} = require('../lib');

const net = require('net');

const echoService = new Service({
  protocol: 'Echo',
  messages: {
    echo: {
      request: [{name: 'message', type: 'string'}],
      response: 'string',
    },
    upper: {
      request: [{name: 'message', type: 'string'}],
      response: 'string',
    },
  },
});

const server = new Server(echoService)
  .use((call, next) => {
    next();
  })
  .onMessage().upper((str, cb) => {
    cb(null, str.toUpperCase());
  });

const bridge = new channels.NettyServerBridge(server.channel);

net.createServer()
  .on('connection', (conn) => {
    conn
      .on('error', (err) => { console.error(`Connection failure: ${err}`); })
      .on('unpipe', () => {
        console.log('Connection unpiped.');
        conn.destroy();
      });
    bridge.accept(conn);
  })
  .listen(8080, () => { console.log('Listening...'); });
