/* jshint node: true, mocha: true */

'use strict';

var Tap = require('../lib/tap'),
    assert = require('assert');

suite('tap', function () {

  suite('long', function () {

    test('write', function () {

      var tap = newTap(20);
      tap.writeLong(1440756011948);
      var buf = new Buffer(['0xd8', '0xce', '0x80', '0xbc', '0xee', '0x53']);
      assert(buf.equals(tap.buf.slice(0, 6)));

    });

    test('read', function () {

      var buf = new Buffer(['0xd8', '0xce', '0x80', '0xbc', '0xee', '0x53']);
      assert.equal((new Tap(buf)).readLong(), 1440756011948);

    });

    test('write read', function () {

      var tap = newTap(20);
      var nums = [0, 3, 1234, -5, 1440756011948];
      var i, l, num;
      for (i = 0, l = nums.length; i < l; i++) {
        tap.buf.fill(0);
        tap.offset = 0;
        num = nums[i];
        tap.writeLong(num);
        tap.offset = 0;
        assert.equal(tap.readLong(), num);
      }

    });

  });

  suite('string', function () {

    test('write read', function () {

      var tap = newTap(20);
      tap.writeString('hello!');
      tap.offset = 0;
      assert.equal(tap.readString(), 'hello!');

    });

    test('write too long', function () {

      var tap = newTap(5);
      tap.writeString('hello there!');
      assert(!tap.isValid());

    });

  });

  suite('fixed', function () {

    test('write read', function () {

      var tap = newTap(20);
      var fixed = new Buffer('abc');
      tap.writeFixed(fixed);
      tap.offset = 0;
      assert.deepEqual(tap.readFixed(3), fixed);

    });

    test('write too long', function () {

      var tap = newTap(3);
      tap.writeFixed(new Buffer('abcde'));
      assert(!tap.isValid());

    });

  });

  suite('array', function () {

    var tap = newTap(100);

    var tests = [
      {
        name: 'long',
        arr: [1, 3, -4, 7],
        fns: [tap.writeLong, tap.readLong]
      },
      {
        name: 'string',
        arr: ['hi', 'james', '!'],
        fns: [tap.writeString, tap.readString]
      },
      {
        name: 'empty',
        arr: [],
        fns: [tap.writeLong, tap.readLong],
      },
      {
        name: 'array of long',
        arr: [[3, 4], [], [1, 2]],
        fns: [
          function (arr) { tap.writeArray(arr, tap.writeLong); },
          function () { return tap.readArray(tap.readLong); }
        ]
      }
    ];

    tests.forEach(function (obj) {
      test(obj.name, function () {
        tap.offset = 0;
        tap.writeArray(obj.arr, obj.fns[0]);
        tap.offset = 0;
        assert.deepEqual(tap.readArray(obj.fns[1]), obj.arr);
      });
    });

  });

  suite('map', function () {

    var tap = newTap(100);

    var tests = [
      {
        name: 'long',
        obj: {one: 1, foo: -1231},
        fns: [tap.writeLong, tap.readLong]
      },
      {
        name: 'string',
        obj: {foo: 'hi', 'foo.bar': 'james', '': '!'},
        fns: [tap.writeString, tap.readString]
      },
      {
        name: 'empty',
        obj: {},
        fns: [tap.writeLong, tap.readLong],
      },
      {
        name: 'array of long',
        obj: {one: [3, 4], two: [], three: [1, 2]},
        fns: [
          function (obj) { tap.writeArray(obj, tap.writeLong); },
          function () { return tap.readArray(tap.readLong); }
        ]
      }
    ];

    tests.forEach(function (obj) {
      test(obj.name, function () {
        tap.offset = 0;
        tap.writeMap(obj.obj, obj.fns[0]);
        tap.offset = 0;
        assert.deepEqual(tap.readMap(obj.fns[1]), obj.obj);
      });
    });

  });

});

function newTap(n) {

  var buf = new Buffer(n);
  buf.fill(0);
  return new Tap(buf);

}
