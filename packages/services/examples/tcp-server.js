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

const echoServer = new Server(echoService)
  .onMessage().echo((str, cb) => { cb(null, str); });

const upperServer = new Server(upperService)
  .onMessage().upper((str, cb) => { cb(null, str.toUpperCase()); });

const router = new channels.Router([echoServer.channel, upperServer.channel]);
const bridge = new channels.NettyServerBridge(router.channel);

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
