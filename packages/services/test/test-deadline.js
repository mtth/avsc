/* jshint esversion: 6, mocha: true, node: true */

'use strict';

const {Deadline} = require('../lib/deadline');

const assert = require('assert');
const sinon = require('sinon');

suite('Deadline', () => {
  let clock;

  setup(() => { clock = sinon.useFakeTimers(); });
  teardown(() => { clock.restore(); });

  test('expire with default error', (done) => {
    const deadline = Deadline.infinite();
    deadline.whenExpired((err) => {
      assert.ifError(err);
      assert(deadline.expired);
      done();
    });
    deadline.expire();
  });

  test('expire with custom error', (done) => {
    const deadline = Deadline.infinite();
    const cause = new Error('foo');
    deadline.whenExpired((err) => {
      assert.strictEqual(err, cause);
      done();
    });
    deadline.expire(cause);
  });

  test('deadline exceeded', (done) => {
    const deadline = Deadline.forMillis(50);
    deadline.whenExpired((err) => {
      assert.equal(err.code, 'ERR_DEADLINE_EXCEEDED');
      assert(deadline.expired);
      done();
    });
    clock.tick(25);
    assert(!deadline.expired);
    clock.tick(55);
  });

  test('deadline propagates to child', () => {
    const parent = Deadline.forMillis(50);
    const child = Deadline.infinite(parent);
    assert(child.expiration.equals(parent.expiration));
  });

  test('child adjusts parent deadline', () => {
    const parent = Deadline.forMillis(50);
    const child = Deadline.forMillis(40, parent);
    assert.equal(+child.remainingDuration, 40);
  });

  test('expire propagates to child', (done) => {
    const parent = Deadline.infinite();
    const child = Deadline.infinite(parent);
    child.whenExpired(() => {
      assert(child.expired);
      assert(parent.expired);
      done();
    });
    parent.expire();
    parent.expire();
  });

  test('expire does not propagate from child', (done) => {
    const parent = Deadline.infinite();
    const child = Deadline.forMillis(10, parent);
    child.whenExpired(() => {
      assert(child.expired);
      assert(!parent.expired);
      done();
    });
    child.expire();
  });

  test('timeout child', (done) => {
    const parent = Deadline.forMillis(10);
    const child = Deadline.infinite(parent);
    child.whenExpired(() => {
      assert(child.expired);
      assert(child.expired);
      done();
    });
    clock.tick(15);
  });

  test('expired calls handler immediately', () => {
    const deadline = Deadline.infinite();
    deadline.expire();
    let expired = false;
    deadline.whenExpired(() => { expired = true; });
    assert(expired);
  });

  test('wrap without deadline', (done) => {
    const deadline = Deadline.infinite();
    const wrapped = deadline.wrap(done);
    wrapped();
  });

  test('wrap callback finishes first', (done) => {
    const deadline = Deadline.forMillis(10);
    const wrapped = deadline.wrap((err, str) => {
      assert.ifError(err);
      assert.equal(str, 'foo');
      clock.tick(15);
      done();
    });
    wrapped(null, 'foo');
  });

  test('wrap deadline finishes first', (done) => {
    const deadline = Deadline.forMillis(10);
    const wrapped = deadline.wrap((err) => {
      assert.equal(err.code, 'ERR_DEADLINE_EXCEEDED');
      clock.tick(10);
      done();
    });
    setTimeout(() => { wrapped(new Error('boom')); }, 20);
    clock.tick(15);
  });

  test('wrap deadline already expired', (done) => {
    const deadline = Deadline.infinite();
    deadline.expire();
    deadline.wrap((err) => {
      assert.strictEqual(err, null);
      done();
    });
  });
});
