/* jshint esversion: 6, mocha: true, node: true */

'use strict';

const {Message, Service} = require('../lib/service');

const assert = require('assert');

suite('Service', () => {
  test('empty', () => {
    const svc = new Service({protocol: 'Foo'});
    assert.equal(svc.name, 'Foo');
  });

  test('messages', () => {
    const svc = new Service({
      protocol: 'Echo',
      types: [
        {name: 'Empty', type: 'record', fields: []},
      ],
      messages: {
        ping: {request: [], response: 'Empty'},
      },
    });
    assert.equal(svc.name, 'Echo');
    const msg = svc.messages.get('ping');
    assert.equal(msg.name, 'ping');
    assert.equal(msg.response.name, 'Empty');
    assert.equal(msg.error.types[0].typeName, 'logical:system-error');
    assert.deepEqual(msg.schema(), {
      request:[],
      response: {name: 'Empty', type: 'record', fields: []},
    });
  });

  test('compatible', () => {
    const svc = new Service({
      protocol: 'Echo',
      types: [
        {name: 'Empty', type: 'record', fields: []},
      ],
      messages: {
        ping: {request: [], response: 'Empty'},
      },
    });
    assert(Service.compatible(svc, svc));
    assert(!Service.compatible(svc, new Service({protocol: 'Echo'})));
  });
});
