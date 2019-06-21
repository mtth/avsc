/* jshint esversion: 8, mocha: true, node: true */

'use strict';

const {PromisifiedServer, PromisifiedDeadline} = require('../lib/promises');
const {Service} = require('../lib/service');

const assert = require('assert');

suite('promises', () => {
  suite('deadline', () => {
    test('expiration with error', async () => {
      const deadline = PromisifiedDeadline.infinite();
      const cause = new Error('boom');
      deadline.expire(cause);
      await deadline.whenExpired().catch((err) => {
        assert.strictEqual(err, cause);
      });
    });

    test('manual expiration', async () => {
      const deadline = PromisifiedDeadline.infinite();
      deadline.expire();
      await deadline.whenExpired();
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
      return client.emitMessage().upper('foo')
        .then(function (res) {
          assert.strictEqual(this.client, client);
          assert.equal(res, 'FOO');
        });
    });

    test('application error', async () => {
      const {client, server} = clientServer(echoSvc);
      server.onMessage().upper(() => {
        const err = new Error('boom');
        err.code = 'ERR_BOOM';
        throw err;
      });
      try {
        await client.emitMessage().upper('foo');
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
      return client.emitMessage().upper('foo')
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
        .onMessage().upper((msg) => msg);
      client
        .use(async (wreq, wres, next) => {
          evts.push('client mw in');
          await next();
          evts.push('client mw out');
        });
      const res = await client.emitMessage().upper('foo');
      assert.equal(res, 'foo');
      assert.deepEqual(evts, [
        'client mw in',
        'server mw in',
        'server mw out',
        'client mw out',
      ]);
    });

    test('middleware error', async () => {
      const {client, server} = clientServer(echoSvc);
      const boom = new Error();
      server
        .use(async (wreq, wres, next) => {
          try {
            await next();
          } catch (err) {
            assert.strictEqual(err, boom);
            wres.response = 'bar';
            return;
          }
          assert(false);
        })
        .onMessage().upper(() => { throw boom; });
      const res = await client.emitMessage().upper('foo');
      assert.equal(res, 'bar');
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
      const str = await client.emitMessage().echo('abc');
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
      const ok = await client.emitMessage().ping();
      assert.strictEqual(ok, null);
    });


    test('middleware simple case', async () => {
      const {client, server} = clientServer(echoSvc);
      const evts = [];
      server
        .use(() => { evts.push('server'); })
        .onMessage().upper((msg) => msg);
      client.use(() => { evts.push('client'); });
      const res = await client.emitMessage().upper('bar');
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
