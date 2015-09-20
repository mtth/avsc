/* jshint node: true, mocha: true */

'use strict';

var utils = require('../lib/utils'),
    assert = require('assert');

suite('utils', function () {

  test('capitalize', function () {
    assert.equal(utils.capitalize('abc'), 'Abc');
    assert.equal(utils.capitalize(''), '');
    assert.equal(utils.capitalize('aBc'), 'ABc');
  });

  test('hasDuplicates', function () {
    assert(utils.hasDuplicates([1, 3, 1]));
    assert(!utils.hasDuplicates([]));
    assert(!utils.hasDuplicates(['ab', 'cb']));
    assert(utils.hasDuplicates(['ab', 'cb'], function (s) { return s[1]; }));
  });

  suite('ConsecutiveQueue', function () {

    test('in order', function () {
      var q = new utils.ConsecutiveQueue();
      assert.equal(q.next(), null);
      q.add(0, 'hello');
      assert.equal(q.size(), 1);
      assert.equal(q.next(), 'hello');
      q.add(1, 'hi');
      assert.equal(q.next(), 'hi');
      assert.equal(q.next(), null);
      assert.equal(q.size(), 0);
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
      assert.equal(q.size(), 2);
      q.add(0, 'hey');
      assert.equal(q.size(), 3);
      assert.equal(q.next(), 'hey');
      assert.equal(q.next(), 'hi');
      assert.equal(q.next(), 'hello');
      assert.equal(q.next(), null);
    });

    test('decreasing index', function () {
      var q = new utils.ConsecutiveQueue();
      q.add(0, 'hey');
      q.add(1, 'hi');
      q.next();
      assert.throws(function () { q.add(0, 'hey'); });
    });

  });

  suite('Lcg', function () {

    test('seed', function () {
      var r1 = new utils.Lcg(48);
      var r2 = new utils.Lcg(48);
      assert.equal(r1.nextInt(), r2.nextInt());
    });

    test('integer', function () {
      var r = new utils.Lcg(48);
      var i;
      i = r.nextInt();
      assert(i >= 0 && i === (i | 0));
      i = r.nextInt(1);
      assert.equal(i, 0);
      i = r.nextInt(1, 2);
      assert.equal(i, 1);
    });

    test('float', function () {
      var r = new utils.Lcg(48);
      var f;
      f = r.nextFloat();
      assert(0 <= f && f < 1);
      f = r.nextFloat(0);
      assert.equal(f, 0);
      f = r.nextFloat(1, 1);
      assert.equal(f, 1);
    });

    test('boolean', function () {
      var r = new utils.Lcg(48);
      assert(typeof r.nextBoolean() == 'boolean');
    });

    test('choice', function () {
      var r = new utils.Lcg(48);
      var arr = ['a'];
      assert(r.choice(arr), 'a');
      assert.throws(function () { r.choice([]); });
    });

    test('string', function () {
      var r = new utils.Lcg(48);
      var s;
      s = r.nextString(10, 'aA#!');
      assert.equal(s.length, 10);
      s = r.nextString(5, '#!');
      assert.equal(s.length, 5);
    });

  });

});
