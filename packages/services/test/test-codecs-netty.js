/* jshint esversion: 6, mocha: true, node: true */

'use strict';

const {Client, Server} = require('../lib/call');
const {Trace} = require('../lib/trace');
const {NettyChannel, NettyGateway} = require('../lib/codecs/netty');
const {Service} = require('../lib/service');

const {Type} = require('@avro/types');
const assert = require('assert');
const {DateTime} = require('luxon');
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
      client.emitMessage(new Trace())
        .upper('foo', (err1, err2, res) => {
          assert(!err1, err1);
          assert(!err2, err2);
          assert.equal(res, 'FOO');
          done();
        });
    });

    test('expire trace', (done) => {
      const trace = new Trace();
      server.onMessage().upper((msg, cb) => {
        trace.expire();
      });
      client.emitMessage(trace).upper('foo', (err) => {
        assert.equal(err.code, 'ERR_AVRO_EXPIRED');
        done();
      });
    });

    test('trace deadline and label propagation', (done) => {
      const deadline = DateTime.fromMillis(1234);
      const uuid = 'abc';
      let ok = false;
      server
        .use(function (wreq, wres, next) {
          assert.equal(+this.trace.deadline, +deadline);
          assert.deepEqual(this.trace.labels, {uuid});
          ok = true;
          next();
        })
        .onMessage().echo((str, cb) => { cb(null, str); });
      const trace = new Trace(deadline);
      trace.labels.uuid = uuid;
      client.emitMessage(trace).echo('foo', (err) => {
        assert(!err, err);
        assert(ok);
        done();
      });
    });

    test('header propagation', (done) => {
      const type = Type.forSchema('int');
      client.tagTypes.cost = type;
      server.tagTypes.cost = type;
      server
        .use((wreq, wres, next) => {
          wres.tags.cost = wreq.tags.cost + 1;
          next();
        })
        .onMessage().echo((str, cb) => { cb(null, str); });
      let cost = 0;
      client
        .use((wreq, wres, next) => {
          wreq.tags.cost = 1;
          next(null, (err, prev) => {
            cost = wres.tags.cost;
            prev(err);
          });
        })
        .emitMessage(new Trace()).echo('foo', (err, res) => {
          assert.ifError(err);
          assert.equal(res, 'foo');
          assert.equal(cost, 2);
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
        client.emitMessage(new Trace()).echo('foo', (err, str) => {
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
        server.onMessage().echo(() => { assert(false); }); // Not called.
        client.emitMessage(new Trace()).echo('', (err) => {
          assert.equal(err.code, 'ERR_AVRO_INCOMPATIBLE_PROTOCOL');
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
        conn.destroy();
        tcpServer.close();
      });
      const client = new Client(clientSvc).channel(chan);
      cb(null, {client, server});
    });
}
