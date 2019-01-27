/* jshint esversion: 6, mocha: true, node: true */

'use strict';

const {Trace} = require('../lib/channel');

const assert = require('assert');
const sinon = require('sinon');

suite('Trace', () => {
  let clock;

  setup(() => { clock = sinon.useFakeTimers(); });
  teardown(() => { clock.restore(); });

  test('expire with default error', (done) => {
    const trace = new Trace();
    trace.onceInactive((err) => {
      assert.equal(err.code, 'ERR_AVRO_EXPIRED');
      assert(!trace.active);
      done();
    })
    trace.expire();
  });

  test('expire with custom error', (done) => {
    const trace = new Trace();
    const cause = new Error('foo');
    trace.onceInactive((err) => {
      assert.equal(err.code, 'ERR_AVRO_EXPIRED');
      assert.strictEqual(err.cause, cause);
      done();
    })
    trace.expire(cause);
  });

  test('deadline exceeded', (done) => {
    const trace = new Trace(50);
    trace.onceInactive((err) => {
      assert.equal(err.code, 'ERR_AVRO_DEADLINE_EXCEEDED');
      assert(!trace.active);
      done();
    });
    clock.tick(25);
    assert(trace.active);
    clock.tick(55);
  });

  test('expire child', (done) => {
    const parent = new Trace();
    const child = new Trace(parent);
    child.onceInactive(() => {
      assert(!child.active);
      assert(!parent.active);
      done();
    })
    parent.expire();
  });
});
