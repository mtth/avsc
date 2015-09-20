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

  suite('Lcg', function () {

    test('seed', function () {
      var r1 = new utils.Lcg(48);
      var r2 = new utils.Lcg(48);
      assert.equal(r1.nextInt(), r2.nextInt());
    });

    test('integer', function () {
      var r = new utils.Lcg(48);
      var i = r.nextInt();
      assert(i >= 0 && i === (i | 0));
    });

    test('choice', function () {
      var r = new utils.Lcg(48);
      var arr = ['a'];
      assert(r.choice(arr), 'a');
    });

    test('string', function () {
      var r = new utils.Lcg(48);
      var s = r.nextString(10);
      assert.equal(s.length, 10);
    });

  });

});
