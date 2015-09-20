/* jshint node: true, mocha: true */

'use strict';

var utils = require('../lib/utils'),
    assert = require('assert');

suite('utils', function () {

  suite('ConsecutiveQueue', function () {

    test('in order', function () {
      var q = new utils.ConsecutiveQueue();
      assert.equal(q.next(), null);
      q.add(0, 'hello');
      assert.equal(q.next(), 'hello');
      q.add(1, 'hi');
      assert.equal(q.next(), 'hi');
      assert.equal(q.next(), null);
    });

    test('single out of order', function () {
      var q = new utils.ConsecutiveQueue();
      q.add(1, 'hi');
      assert.equal(q.next(), null);
      q.add(0, 'hello');
      assert.equal(q.next(), 'hello');
      assert.equal(q.next(), 'hi');
      assert.equal(q.next(), null);
    });

    test('multiple out of order', function () {
      var q = new utils.ConsecutiveQueue();
      assert.equal(q.next(), null);
      q.add(2, 'hello');
      q.add(1, 'hi');
      assert.equal(q.next(), null);
      q.add(0, 'hey');
      assert.equal(q.next(), 'hey');
      assert.equal(q.next(), 'hi');
      assert.equal(q.next(), 'hello');
      assert.equal(q.next(), null);
    });

  });

});
