/* jshint esversion: 6, mocha: true, node: true */

'use strict';

const {Client, Server} = require('../lib/call');
const {NettyChannel, NettyGateway} = require('../lib/codecs/netty');
const {Deadline} = require('../lib/deadline');
const {Service} = require('../lib/service');

const assert = require('assert');
const net = require('net');
const sinon = require('sinon');

suite('netty codec', () => {
  let clock;
  setup(() => { clock = sinon.useFakeTimers(); });
  teardown(() => { clock.restore(); });

  const stringService = new Service({
    protocol: 'StringService',
    messages: {
      echo: {
        request: [{name: 'message', type: 'string'}],
        response: 'string',
      },
      upper: {
        request: [{name: 'message', type: 'string'}],
        response: 'string',
        errors: [
          {name: 'UpperError', type: 'error', fields: []},
        ],
      },
    },
  });

  suite('same service', () => {
    let client, server;

    setup((done) => {
      routing(stringService, stringService, (err, obj) => {
        if (err) {
          done(err);
          return;
        }
        client = obj.client;
        server = obj.server;
        done();
      });
    });

    teardown(() => { client.channel().close(); });

    test('emit and receive message', (done) => {
      server.onMessage()
        .upper((msg, cb) => {
          cb(null, null, msg.toUpperCase());
        });
      client.emitMessage()
        .upper('foo', (err1, err2, res) => {
          assert(!err1, err1);
          assert(!err2, err2);
          assert.equal(res, 'FOO');
          done();
        });
    });

    test('expire deadline', (done) => {
      const deadline = Deadline.infinite();
      server.onMessage().upper((msg, cb) => {
        deadline.expire();
        cb();
      });
      client.emitMessage(deadline).upper('foo', (err) => {
        assert.equal(err.code, 'ERR_DEADLINE_EXPIRED');
        done();
      });
    });

    test('deadline deadline and baggage propagation', (done) => {
      const deadline = Deadline.forMillis(1234);
      const uuid = 'abc';
      let ok = false;
      server
        .use(function (wreq, wres, next) {
          assert.equal(+this.deadlineExpiration, +deadline.expiration);
          assert.deepEqual(this.baggages, {uuid});
          ok = true;
          next();
        })
        .onMessage().echo((str, cb) => { cb(null, str); });
      client.emitMessage({deadline, baggages: {uuid}})
        .echo('foo', function (err) {
          assert(!err, err);
          assert(ok);
          done();
        });
    });

    test('header propagation', (done) => {
      server
        .use((wreq, wres, next) => {
          wres.headers.chars = wreq.headers.chars + 'b';
          next();
        })
        .onMessage().echo((str, cb) => { cb(null, str); });
      let chars = 0;
      client
        .use((wreq, wres, next) => {
          wreq.headers.chars = 'a';
          next(null, (err, prev) => {
            chars = wres.headers.chars;
            prev(err);
          });
        })
        .emitMessage().echo('foo', (err, res) => {
          assert.ifError(err);
          assert.equal(res, 'foo');
          assert.equal(chars, 'ab');
          done();
        });
    });
  });

  suite('different services', () => {
    test('compatible', (done) => {
      const echoService = new Service({
        protocol: 'StringService',
        messages: {
          echo: {
            request: [{name: 'message', type: 'string'}],
            response: 'string',
          },
        },
      });
      routing(echoService, stringService, (err, obj) => {
        if (err) {
          done(err);
          return;
        }
        const {client, server} = obj;
        server.onMessage().echo((str, cb) => { cb(null, str); });
        client.emitMessage().echo('foo', (err, str) => {
          assert.ifError(err);
          assert.equal(str, 'foo');
          client.channel().close();
          done();
        });
      });
    });

    test('incompatible', (done) => {
      const svc = new Service({
        protocol: 'StringService',
        messages: {
          echo: {
            request: [{name: 'data', type: 'string'}], // Different name.
            response: 'string',
          },
        },
      });
      routing(svc, stringService, (err, obj) => {
        if (err) {
          done(err);
          return;
        }
        const {client, server} = obj;
        server.onMessage().echo((msg, cb) => { // Not called.
          assert(false);
          cb();
        });
        client.emitMessage().echo('', (err) => {
          assert.equal(err.code, 'ERR_INCOMPATIBLE_PROTOCOL');
          client.channel().close();
          done();
        });
      });
    });
  });
});

function routing(clientSvc, serverSvc, cb) {
  const server = new Server(serverSvc);
  const gateway = new NettyGateway(server.channel());
  net.createServer()
    .on('connection', (conn) => { gateway.accept(conn); })
    .listen(0, function () {
      const tcpServer = this;
      const port = tcpServer.address().port;
      const conn = net.createConnection(port).setNoDelay();
      const chan = new NettyChannel(conn).on('close', () => {
        conn.end();
        tcpServer.close();
      });
      const client = new Client(clientSvc).channel(chan);
      cb(null, {client, server});
    });
}
