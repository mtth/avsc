/* jshint esversion: 6, mocha: true, node: true */

'use strict';

const {Trace} = require('../lib/trace');

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

  test('deadline propagates to child', () => {
    const parent = new Trace(50);
    const child = new Trace(parent);
    assert(child.deadline.equals(parent.deadline));
  });

  test('child adjusts parent deadline', () => {
    const parent = new Trace(50);
    const child = new Trace(40, parent);
    assert.equal(+child.remainingDuration, 40);
  });

  test('expire propagates to child', (done) => {
    const parent = new Trace();
    const child = new Trace(parent);
    child.onceInactive(() => {
      assert(!child.active);
      assert(!parent.active);
      done();
    })
    parent.expire();
    parent.expire();
  });

  test('expire does not propagate from child', (done) => {
    const parent = new Trace();
    const child = new Trace(10, parent);
    child.onceInactive(() => {
      assert(!child.active);
      assert(parent.active);
      done();
    })
    child.expire();
  });

  test('timeout child', (done) => {
    const parent = new Trace(10);
    const child = new Trace(parent);
    child.onceInactive(() => {
      assert(!child.active);
      assert(!parent.active);
      done();
    })
    clock.tick(15);
  });

  test('inactive calls handler immediately', () => {
    const trace = new Trace();
    trace.expire();
    let expired = false;
    trace.onceInactive(() => { expired = true; });
    assert(expired);
  });

  test('timeout free child', (done) => {
    const parent = new Trace(10);
    const child = new Trace(parent, {free: true});
    child.onceInactive(() => {
      done();
    })
    clock.tick(15);
    assert(!parent.active);
    assert(child.active);
    child.expire();
  });

  test('child headers', () => {
    const parent = new Trace(null, {headers: {one: '1', two: '2'}});
    const child = new Trace(parent, {headers: {two: '22'}});
    assert.equal(child.headers.one, '1');
    assert.equal(child.headers.two, '22');
    assert.equal(parent.headers.two, '2');
  });
});
