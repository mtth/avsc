/* jshint esversion: 6, node: true */

'use strict';

const {PromiseServer} = require('../lib/promises');
const {Service} = require('../lib/service');
const {Trace} = require('../lib/trace');

const {Type} = require('@avro/types');
const assert = require('assert');

suite('promises', () => {
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

  test('application error', () => {
    const {client, server} = clientServer(echoSvc);
    const UpperError = echoSvc.types.get('UpperError').recordConstructor;
    server.onMessage().upper(() => {
      const err = new Error('boom');
      err.code = 'ERR_BOOM';
      throw err;
    });
    return client.emitMessage(new Trace()).upper('foo')
      .catch((err) => {
        assert.equal(err.code, 'ERR_AVRO_APPLICATION');
        assert.equal(err.cause.message, 'boom');
        assert.equal(err.applicationCode, 'ERR_BOOM');
      });
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

  test('middleware ok', () => {
    const {client, server} = clientServer(echoSvc);
    const evts = [];
    server
      .use(async (wreq, wres, next) => {
        evts.push('server mw in');
        await next();
        evts.push('server mw out');
      })
      .onMessage().upper((msg) => msg)
    return client
      .use(async (wreq, wres, next) => {
        evts.push('client mw in');
        await next();
        evts.push('client mw out');
      })
      .emitMessage(new Trace()).upper('foo')
      .then((res) => {
        assert.equal(res, 'foo');
        assert.deepEqual(evts, [
          'client mw in',
          'server mw in',
          'server mw out',
          'client mw out',
        ]);
      });
  });

  test('middleware simple case', () => {
    const {client, server} = clientServer(echoSvc);
    const evts = [];
    server
      .use(() => { evts.push('server'); })
      .onMessage().upper((msg) => msg)
    return client
      .use(() => { evts.push('client'); })
      .emitMessage(new Trace()).upper('bar')
      .then((res) => {
        assert.equal(res, 'bar');
        assert.deepEqual(evts, ['client', 'server']);
      });
  });
});

function clientServer(svc) {
  const server = new PromiseServer(svc);
  const client = server.client();
  return {client, server};
}
