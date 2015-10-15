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

  test('single index of', function () {
    assert.equal(utils.singleIndexOf(null, 1), -1);
    assert.equal(utils.singleIndexOf([2], 2), 0);
    assert.equal(utils.singleIndexOf([3, 3], 3), -2);
    assert.equal(utils.singleIndexOf([2, 4], 4), 1);
  });

  test('abstract function', function () {
    assert.throws(utils.abstractFunction, utils.Error);
  });

  suite('OrderedQueue', function () {

    var seqs = [
      [0],
      [0,1],
      [0,1,2],
      [2,1,0],
      [0,2,1,3],
      [1,3,2,4,0],
      [0,1,2,3]
    ];

    var i;
    for (i = 0; i < seqs.length; i++) {
      check(seqs[i]);
    }

    function check(seq) {
      var q = new utils.OrderedQueue();
      var i;
      assert.strictEqual(q.pop(), null);
      for (i = 0; i < seq.length; i++) {
        q.push({index: seq[i]});
      }
      for (i = 0; i < seq.length; i++) {
        var j = q.pop();
        assert.equal(j !== null && j.index, i, seq.join());
      }
    }

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
