/* jshint esversion: 6, mocha: true, node: true */

'use strict';

const {Client, Server} = require('../lib/call');
const {Context} = require('../lib/context');
const {Service} = require('../lib/service');

const assert = require('assert');
const {Type} = require('avsc');

suite('client server', () => {
  const echoSvc = new Service({
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

  test('simple', (done) => {
    const {client, server} = clientServer(echoSvc);
    server.onMessage()
      .upper((msg, cb) => {
        cb(null, null, msg.toUpperCase());
      });
    client.emitMessage(new Context())
      .upper('foo', (err1, err2, res) => {
        assert(!err1, err1);
        assert(!err2, err2);
        assert.equal(res, 'FOO');
        done();
      });
  });

  test('cancel', (done) => {
    const {client, server} = clientServer(echoSvc);
    const ctx = new Context();
    server.onMessage().upper((msg, cb) => {
      ctx.interrupt();
    });
    client.emitMessage(ctx).upper('foo', (err) => {
      assert.equal(err.code, 'ERR_AVRO_INTERRUPTED');
      done();
    });
  });

  test('ping', (done) => {
    const {client, server} = clientServer(echoSvc);
    client.ping(new Context(), (err) => {
      assert(!err, err);
      done();
    });
  });

  test('middleware with handler', (done) => {
    const {client, server} = clientServer(echoSvc);
    const evts = [];
    server
      .use((call, next) => {
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
      .use((call, next) => {
        evts.push('client mw in');
        next(null, (err, prev) => {
          evts.push('client mw out');
          prev(err);
        });
      })
      .emitMessage(new Context()).echo('foo', (err, res) => {
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
    const {client, server} = clientServer(echoSvc);
    const intType = Type.forSchema('int');
    client.tagTypes.attempt = intType;
    server.tagTypes.attempt = intType;
    server
      .use((call, next) => {
        if (call.tags.attempt === 1) {
          next();
          return;
        }
        next(new Error('try again'));
      })
      .onMessage().echo((msg, cb) => {
        cb(null, msg);
      });
    client
      .use((call, next) => {
        const attempt = call.tags.attempt;
        if (attempt === undefined) {
          call.tags.attempt = 0;
        } else {
          call.tags.attempt = attempt + 1;
        }
        next();
      })
      .emitMessage(new Context(), retry(1)).echo('foo', (err, res) => {
        assert(!err, err);
        assert.equal(res, 'foo');
        done();
      });

    function retry(n) {
      return (call, next) => {
        tryOnce(0);

        function tryOnce(i) {
          next(null, (err, prev) => {
            assert(!err, err);
            if (i >= n) {
              prev();
              return;
            }
            call.error = undefined;
            tryOnce(i + 1);
          });
        }
      };
    }
  });

  test('middleware missing handler', (done) => {
    const {client, server} = clientServer(echoSvc);
    const evts = [];
    server
      .use((call, next) => {
        evts.push('server mw in');
        next(null, (err, prev) => {
          evts.push('server mw out');
          prev();
        });
      });
    client
      .use((call, next) => {
        evts.push('client mw in');
        next(null, (err, prev) => {
          evts.push('client mw out');
          prev();
        });
      })
      .emitMessage(new Context()).echo('foo', (err, res) => {
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

function clientServer(svc) {
  const client = new Client(svc);
  const server = new Server(svc);
  client.channel = server.channel;
  return {client, server};
}

function time() {
  return (call, next) => {
    const start = Date.now();
    const cleanup = call.context.onCancel(done);

    next(null, (err, prev) => {
      done();
      prev(err);
    });

    function done() {
      cleanup();
      console.log(`Run in ${Date.now() - start}.`);
    }
  };
}
