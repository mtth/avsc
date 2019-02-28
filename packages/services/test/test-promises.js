/* jshint esversion: 6, node: true */

'use strict';

const {PromisifiedServer, PromisifiedTrace} = require('../lib/promises');
const {Service} = require('../lib/service');
const {Trace} = require('../lib/trace');

const {Type} = require('@avro/types');
const assert = require('assert');

suite('promises', () => {
  suite('trace', () => {
    test('expiration with error', async () => {
      const trace = new PromisifiedTrace();
      const cause = new Error('boom');
      trace.expire(cause);
      await trace.expiration.catch((err) => {
        assert.strictEqual(err, cause);
      });
    });

    test('manual expiration', async () => {
      const trace = new PromisifiedTrace();
      trace.expire();
      await trace.expiration;
    });
  });

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

    test('ok', () => {
      const {client, server} = clientServer(echoSvc);
      server.onMessage().upper(function (msg) {
        assert.strictEqual(this.server, server);
        return msg.toUpperCase();
      });
      return client.emitMessage(new Trace()).upper('foo')
        .then(function (res) {
          assert.strictEqual(this.client, client);
          assert.equal(res, 'FOO');
        });
    });

    test('application error', async () => {
      const {client, server} = clientServer(echoSvc);
      const UpperError = echoSvc.types.get('UpperError').recordConstructor;
      server.onMessage().upper(() => {
        const err = new Error('boom');
        err.code = 'ERR_BOOM';
        throw err;
      });
      try {
        await client.emitMessage(new Trace()).upper('foo');
      } catch (err) {
        assert.equal(err.code, 'ERR_APPLICATION');
        assert.equal(err.cause.message, 'boom');
        assert.equal(err.applicationCode, 'ERR_BOOM');
      }
    });

    test('custom error', () => {
      const {client, server} = clientServer(echoSvc);
      const UpperError = echoSvc.types.get('UpperError').recordConstructor;
      server.onMessage().upper(() => { throw new UpperError(); });
      return client.emitMessage(new Trace()).upper('foo')
        .catch((err) => {
          assert.strictEqual(err.constructor, UpperError);
        });
    });

    test('middleware ok', async () => {
      const {client, server} = clientServer(echoSvc);
      const evts = [];
      server
        .use(async (wreq, wres, next) => {
          evts.push('server mw in');
          await next();
          evts.push('server mw out');
        })
        .onMessage().upper((msg) => msg)
      client
        .use(async (wreq, wres, next) => {
          evts.push('client mw in');
          await next();
          evts.push('client mw out');
        })
      const res = await client.emitMessage(new Trace()).upper('foo');
      assert.equal(res, 'foo');
      assert.deepEqual(evts, [
        'client mw in',
        'server mw in',
        'server mw out',
        'client mw out',
      ]);
    });

    test('omit optional argument', async () => {
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
      server.onMessage().echo((str) => str);
      const str = await client.emitMessage(new Trace()).echo('abc');
      assert.equal(str, 'abc');
    });

    test('return undefined from one-way handler', async () => {
      const {client, server} = clientServer(new Service({
        protocol: 'Ping',
        messages: {
          ping: {
            request: [],
            response: 'null',
            'one-way': true,
          },
        },
      }));
      server.onMessage().ping(() => {});
      const ok = await client.emitMessage(new Trace()).ping();
      assert.strictEqual(ok, null);
    });


    test('middleware simple case', async () => {
      const {client, server} = clientServer(echoSvc);
      const evts = [];
      server
        .use(() => { evts.push('server'); })
        .onMessage().upper((msg) => msg)
      client.use(() => { evts.push('client'); });
      const res = await client.emitMessage(new Trace()).upper('bar');
      assert.equal(res, 'bar');
      assert.deepEqual(evts, ['client', 'server']);
    });
  });
});

function clientServer(svc) {
  const server = new PromisifiedServer(svc);
  const client = server.client();
  return {client, server};
}
