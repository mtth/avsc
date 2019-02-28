/* jshint esversion: 6, mocha: true, node: true */

'use strict';

const {Server} = require('../lib/call');
const {Service} = require('../lib/service');
const {Trace} = require('../lib/trace');

const {Type} = require('@avro/types');
const assert = require('assert');

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
      ping: {
        request: [{name: 'beat', type: 'int'}],
        response: 'null',
        'one-way': true,
      },
    },
  });

  test('simple', (done) => {
    const {client, server} = clientServer(echoSvc);
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
    const {client, server} = clientServer(echoSvc);
    const trace = new Trace();
    server.onMessage().upper((msg, cb) => {
      trace.expire();
    });
    client.emitMessage(trace).upper('foo', (err) => {
      assert.equal(err.code, 'ERR_TRACE_EXPIRED');
      done();
    });
  });

  test('one way message', (done) => {
    const {client, server} = clientServer(echoSvc);
    server.onMessage()
      .ping((beat, cb) => {
        assert.equal(beat, 123);
        cb();
      });
    client.emitMessage(new Trace())
      .ping(123, (err) => {
        assert.ifError(err);
        done();
      });
  });

  test('omit optional argument', (done) => {
    const {client, server} = clientServer(new Service({
      protocol: 'Echo',
      messages: {
        echo: {
          request: [
            {name: 'message', type: 'string'},
            {name: 'option', type: ['null', 'string'], default: null},
          ],
          response: 'string',
        },
      },
    }));
    server.onMessage().echo((str, opt, cb) => { cb(null, str); });
    client.emitMessage(new Trace()).echo('abc', (err, str) => {
      assert.ifError(err);
      assert.equal(str, 'abc');
      done();
    });
  });

  test('handler application error', (done) => {
    const {client, server} = clientServer(echoSvc);
    server.onMessage()
      .echo((str, cb) => {
        const err = new Error('boom');
        err.code = 'ERR_UNAVAILABLE';
        cb(err);
      });
    client.emitMessage(new Trace())
      .echo('abc', (err) => {
        assert.equal(err.applicationCode, 'ERR_UNAVAILABLE');
        done();
      });
  });

  test('middleware application error', (done) => {
    const {client, server} = clientServer(echoSvc);
    server
      .use((wreq, wres, next) => {
        const err = new Error('bar');
        err.code = 'ERR_BAR';
        next(err);
      });
    client
      .emitMessage(new Trace()).echo('foo', (err) => {
        assert.equal(err.applicationCode, 'ERR_BAR');
        done();
      });
  });

  test('closed channel', (done) => {
    const {client, server} = clientServer(echoSvc);
    client.channel().close();
    client.emitMessage(new Trace()).upper('foo', (err) => {
      assert.equal(err.code, 'ERR_CHANNEL_CLOSED');
      done();
    });
  });

  test('middleware with handler', (done) => {
    const {client, server} = clientServer(echoSvc);
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
    const {client, server} = clientServer(echoSvc);
    const intType = Type.forSchema('int');
    server
      .use((wreq, wres, next) => {
        if (wreq.headers.attempt === '1') {
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
        const attempt = wreq.headers.attempt;
        if (attempt === undefined) {
          wreq.headers.attempt = '0';
        } else {
          wreq.headers.attempt = '' + (+attempt + 1);
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
    const {client, server} = clientServer(echoSvc);
    const evts = [];
    server
      .use((wreq, wres, next) => {
        evts.push('server mw in');
        next(null, (err, prev) => {
          evts.push('server mw out');
          prev(err);
        });
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
        assert(!res, res);
        assert.equal(err.code, 'ERR_NOT_IMPLEMENTED');
        assert.deepEqual(evts, [
          'client mw in',
          'server mw in',
          'server mw out',
          'client mw out',
        ]);
        done();
      });
  });

  test('add middleware after message handlers', (done) => {
    const {client, server} = clientServer(echoSvc);
    let called = false;
    server.onMessage()
      .echo((str, cb) => { cb(null, str); })
      .use((wreq, wres, next) => {
        called = true;
        next();
      });
    client.emitMessage(new Trace())
      .echo('abc', (err, str) => {
        assert.ifError(err);
        assert.equal(str, 'abc');
        assert(called);
        done();
      });
  });
});

function clientServer(svc) {
  const server = new Server(svc);
  const client = server.client();
  return {client, server};
}
