/* jshint esversion: 6, mocha: true, node: true */

'use strict';

const {Context} = require('../lib/context');

const assert = require('assert');
const sinon = require('sinon');

suite('context', () => {
  let clock;

  setup(() => { console.log('foo'); clock = sinon.useFakeTimers(); });
  teardown(() => { clock.restore(); });

  test('expire with default error', (done) => {
    const ctx = new Context();
    ctx.onCancel((err) => {
      assert.equal(err.code, 'ERR_AVRO_EXPIRED');
      assert(ctx.cancelled);
      done();
    })
    ctx.expire();
  });

  test('expire with custom error', (done) => {
    const ctx = new Context();
    const cause = new Error('foo');
    ctx.onCancel((err) => {
      assert.equal(err.code, 'ERR_AVRO_EXPIRED');
      assert.strictEqual(err.cause, cause);
      done();
    })
    ctx.expire(cause);
  });

  test('deadline exceeded', (done) => {
    const ctx = new Context(50);
    ctx.onCancel((err) => {
      assert.equal(err.code, 'ERR_AVRO_DEADLINE_EXCEEDED');
      assert(ctx.cancelled);
      done();
    });
    clock.tick(25);
    assert(!ctx.cancelled);
    clock.tick(55);
  });

  test('expire child', (done) => {
    const parent = new Context();
    const child = new Context(parent);
    child.onCancel((err_) => {
      assert(parent.cancelled);
      done();
    })
    parent.expire();
  });
});
