/* jshint esversion: 6, node: true */

'use strict';

const {Server, Service, channels} = require('../lib');

const net = require('net');

const echoService = new Service({
  protocol: 'Echo',
  messages: {
    upper: {
      request: [{name: 'message', type: 'string'}],
      response: 'int',
    },
  },
});

const server = new Server(echoService)
  .onMessage().upper((str, cb) => {
    cb(null, str.toUpperCase());
  });

const proxy = new channels.NettyProxy(server.channel);

net.createServer()
  .on('connection', (conn) => {
    conn
      .on('error', (err) => { console.error(`Connection failure: ${err}`); })
      .on('unpipe', () => {
        console.log('Connection unpiped.');
        conn.destroy();
      });
    proxy.proxy(conn);
  })
  .listen(8080, () => { console.log('Listening...'); });
