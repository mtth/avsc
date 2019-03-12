/* jshint node: true, mocha: true */

'use strict';

var utils = require('../lib/utils'),
    assert = require('assert');

suite('utils', function () {
  test('copy own properties', function () {
    function Obj() { this.a = 1; this.b = 2; }
    Obj.prototype.c = 2;
    var obj1 = new Obj();

    var obj2 = {b: 3};
    utils.copyOwnProperties(obj1, obj2);
    assert.deepEqual(obj2, {a: 1, b: 3});

    var obj3 = {b: 3};
    utils.copyOwnProperties(obj1, obj3, true);
    assert.deepEqual(obj3, {a: 1, b: 2});
  });

  test('OrderedQueue', function () {
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
});
