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
    trace.whenExpired((err) => {
      assert.ifError(err);
      assert(trace.expired);
      done();
    })
    trace.expire();
  });

  test('expire with custom error', (done) => {
    const trace = new Trace();
    const cause = new Error('foo');
    trace.whenExpired((err) => {
      assert.strictEqual(err, cause);
      done();
    })
    trace.expire(cause);
  });

  test('deadline exceeded', (done) => {
    const trace = new Trace(50);
    trace.whenExpired((err) => {
      assert.equal(err.code, 'ERR_AVRO_DEADLINE_EXCEEDED');
      assert(trace.expired);
      done();
    });
    clock.tick(25);
    assert(!trace.expired);
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
    child.whenExpired(() => {
      assert(child.expired);
      assert(parent.expired);
      done();
    })
    parent.expire();
    parent.expire();
  });

  test('expire does not propagate from child', (done) => {
    const parent = new Trace();
    const child = new Trace(10, parent);
    child.whenExpired(() => {
      assert(child.expired);
      assert(!parent.expired);
      done();
    })
    child.expire();
  });

  test('timeout child', (done) => {
    const parent = new Trace(10);
    const child = new Trace(parent);
    child.whenExpired(() => {
      assert(child.expired);
      assert(child.expired);
      done();
    })
    clock.tick(15);
  });

  test('expired calls handler immediately', () => {
    const trace = new Trace();
    trace.expire();
    let expired = false;
    trace.whenExpired(() => { expired = true; });
    assert(expired);
  });

  test('timeout free child', (done) => {
    const parent = new Trace(10);
    const child = new Trace(parent, {free: true});
    child.whenExpired(() => {
      done();
    })
    clock.tick(15);
    assert(parent.expired);
    assert(!child.expired);
    child.expire();
  });

  test('child headers', () => {
    const parent = new Trace(null, {headers: {one: '1', two: '2'}});
    const child = new Trace(parent, {headers: {two: '22'}});
    assert.equal(child.headers.one, '1');
    assert.equal(child.headers.two, '22');
    assert.equal(parent.headers.two, '2');
  });

  test('wrap without deadline', (done) => {
    const trace = new Trace();
    const wrapped = trace.wrap(done);
    wrapped();
  });

  test('wrap callback finishes first', (done) => {
    const trace = new Trace(10);
    const wrapped = trace.wrap((err, str) => {
      assert.ifError(err);
      assert.equal(str, 'foo');
      clock.tick(15);
      done();
    });
    wrapped(null, 'foo');
  });

  test('wrap trace finishes first', (done) => {
    const trace = new Trace(10);
    const wrapped = trace.wrap((err) => {
      assert.equal(err.code, 'ERR_AVRO_DEADLINE_EXCEEDED');
      clock.tick(10);
      done();
    });
    setTimeout(() => { wrapped(new Error('boom')); }, 20);
    clock.tick(15);
  });

  test('wrap trace already expired', (done) => {
    const trace = new Trace();
    trace.expire();
    const wrapped = trace.wrap((err) => {
      assert.strictEqual(err, null);
      done();
    });
  });
});
