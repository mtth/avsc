/* jshint esversion: 6, mocha: true, node: true */

'use strict';

const {Client, Server} = require('../lib/call');
const {Trace} = require('../lib/channel');
const netty = require('../lib/codecs/netty');
const {Router} = require('../lib/router');
const {Service} = require('../lib/service');

const assert = require('assert');
const {Type} = require('avsc');
const net = require('net');
const sinon = require('sinon');

suite('netty client server', () => {
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
        errors: [
          {name: 'UpperError', type: 'error', fields: []},
        ],
      },
    },
  });

  suite('same protocol no routing', () => {
    let client, server, cleanup;

    setup((done) => {
      routing(echoService, (err, obj) => {
        if (err) {
          done(err);
          return;
        }
        client = obj.client;
        server = obj.server;
        cleanup = obj.cleanup;
        done();
      });
    });

    teardown(() => {
      cleanup();
    });

    test('simple', (done) => {
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

    test('cancel', (done) => {
      const trace = new Trace();
      server.onMessage().upper((msg, cb) => {
        trace.expire();
      });
      client.emitMessage(trace).upper('foo', (err) => {
        assert.equal(err.code, 'ERR_AVRO_EXPIRED');
        done();
      });
    });

    test('middleware with handler', (done) => {
      const evts = [];
      server
        .use((wreq, wres, next) => {
          evts.push('server mw in');
          next(null, (err, prev) => {
            evts.push('server mw out');
            prev(err);
          });
        })
        .onMessage().echo((msg, cb) => {
          evts.push('server handler');
          cb(null, msg);
        });
      client
        .use((wreq, wres, next) => {
          evts.push('client mw in');
          next(null, (err, prev) => {
            evts.push('client mw out');
            prev(err);
          });
        })
        .emitMessage(new Trace()).echo('foo', (err, res) => {
          assert(!err, err);
          assert.equal(res, 'foo');
          assert.deepEqual(evts, [
            'client mw in',
            'server mw in',
            'server handler',
            'server mw out',
            'client mw out',
          ]);
          done();
        });
    });

    test('retry middleware', (done) => {
      const intType = Type.forSchema('int');
      client.tagTypes.attempt = intType;
      server.tagTypes.attempt = intType;
      server
        .use((wreq, wres, next) => {
          if (wreq.tags.attempt === 1) {
            next();
            return;
          }
          next(new Error('try again'));
        })
        .onMessage().echo((msg, cb) => {
          cb(null, msg);
        });
      client
        .use((wreq, wres, next) => {
          const attempt = wreq.tags.attempt;
          if (attempt === undefined) {
            wreq.tags.attempt = 0;
          } else {
            wreq.tags.attempt = attempt + 1;
          }
          next();
        })
        .emitMessage(new Trace(), retry(1)).echo('foo', (err, res) => {
          assert(!err, err);
          assert.equal(res, 'foo');
          done();
        });

      function retry(n) {
        return (wreq, wres, next) => {
          tryOnce(0);

          function tryOnce(i) {
            next(null, (err, prev) => {
              assert(!err, err);
              if (i >= n) {
                prev();
                return;
              }
              wres.error = undefined;
              tryOnce(i + 1);
            });
          }
        };
      }
    });

    test('middleware missing handler', (done) => {
      const evts = [];
      server
        .use((wreq, wres, next) => {
          evts.push('server mw in');
          next(null, (err, prev) => {
            evts.push('server mw out');
            prev();
          });
        });
      client
        .use((wreq, wres, next) => {
          evts.push('client mw in');
          next(null, (err, prev) => {
            evts.push('client mw out');
            prev();
          });
        })
        .emitMessage(new Trace()).echo('foo', (err, res) => {
          assert(!res, res);
          assert.equal(err.code, 'ERR_AVRO_NOT_IMPLEMENTED');
          assert.deepEqual(evts, [
            'client mw in',
            'server mw in',
            'server mw out',
            'client mw out',
          ]);
          done();
        });
    });
  });

  suite('timing no routing', () => {
    let clock;
    let client, server, cleanup;

    setup((done) => {
      clock = sinon.useFakeTimers();
      routing(echoService, (err, obj) => {
        if (err) {
          done(err);
          return;
        }
        client = obj.client;
        server = obj.server;
        cleanup = obj.cleanup;
        done();
      });
    });

    teardown(() => {
      clock.restore();
      cleanup();
    });

    test('deadline propagation', (done) => {
      server
        .use((wreq, wres, next) => {
          clock.tick(10); // Exceed deadline.
          next();
        });
      client.emitMessage(new Trace(5)).echo('foo', (err) => {
        assert.equal(err.code, 'ERR_AVRO_DEADLINE_EXCEEDED');
        done();
      });
    });
  });

  suite('same protocol with routing', () => {
    let client, server, cleanup;

    setup((done) => {
      routing(echoService, (err, obj) => {
        if (err) {
          done(err);
          return;
        }
        client = obj.client;
        server = obj.server;
        cleanup = obj.cleanup;
        done();
      });
    });

    teardown(() => {
      cleanup();
    });

    test('simple', (done) => {
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
  });
});

function routing(svc, cb) {
  const client = new Client(svc);
  const server = new Server(svc);
  const gateway = new netty.Gateway(Router.forServers(server));
  net.createServer()
    .on('connection', (conn) => {
      conn.on('unpipe', () => { conn.destroy(); });
      gateway.accept(conn);
    })
    .listen(0, function () {
      const tcpServer = this;
      const port = tcpServer.address().port;
      const conn = net.createConnection(port).setNoDelay();
      netty.router(conn, (err, router) => {
        if (err) {
          cb(err);
          return;
        }
        client.channel = router.channel;
        cb(null, {client, server, cleanup});
      });

      function cleanup() {
        conn.destroy();
        tcpServer.close();
      }
    });
}
