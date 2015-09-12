/* jshint node: true, mocha: true */

'use strict';

var util = require('../lib/util'),
    assert = require('assert');

suite('util', function () {

  suite('long', function () {

    test('write read', function () {

      var obuf = newOffsetBuffer(20);
      var nums = [0, 3, 1234, -5];
      var i, l, num;
      for (i = 0, l = nums.length; i < l; i++) {
        obuf.buf.fill(0);
        obuf.offset = 0;
        num = nums[i];
        util.writeLong(num, obuf);
        obuf.offset = 0;
        assert.equal(util.readLong(obuf), num);
      }

    });

  });

  suite('string', function () {

    test('write read', function () {

      var obuf = newOffsetBuffer(20);
      util.writeString('hello!', obuf);
      obuf.offset = 0;
      assert.equal(util.readString(obuf), 'hello!');

    });

    test('write too long', function () {

      var obuf = newOffsetBuffer(5);
      assert.throws(function () { util.writeString('hello there!', obuf); });

    });

  });

});

function newOffsetBuffer(n) {

  var buf = new Buffer(n);
  buf.fill(0);
  return new util.OffsetBuffer(buf);

}
