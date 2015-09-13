/* jshint node: true, mocha: true */

'use strict';

var flow = require('../lib/flow'),
    assert = require('assert');

suite('flow', function () {

  suite('long', function () {

    test('write read', function () {

      var flow = newFlow(20);
      var nums = [0, 3, 1234, -5];
      var i, l, num;
      for (i = 0, l = nums.length; i < l; i++) {
        flow.buf.fill(0);
        flow.offset = 0;
        num = nums[i];
        flow.writeLong(num);
        flow.offset = 0;
        assert.equal(flow.readLong(), num);
      }

    });

  });

  suite('string', function () {

    test('write read', function () {

      var flow = newFlow(20);
      flow.writeString('hello!');
      flow.offset = 0;
      assert.equal(flow.readString(), 'hello!');

    });

    test('write too long', function () {

      var flow = newFlow(5);
      assert.throws(function () { flow.writeString('hello there!'); });

    });

  });

  suite('fixed', function () {

    test('write read', function () {

      var flow = newFlow(20);
      var fixed = new Buffer('abc');
      flow.writeFixed(fixed);
      flow.offset = 0;
      assert.deepEqual(flow.readFixed(3), fixed);

    });

    test('write too long', function () {

      var flow = newFlow(3);
      assert.throws(function () { flow.writeFixed(new Buffer('abcde')); });

    });

  });

  suite('array', function () {

    var flow = newFlow(100);

    var tests = [
      {
        name: 'long',
        arr: [1, 3, -4, 7],
        fns: [flow.writeLong, flow.readLong]
      },
      {
        name: 'string',
        arr: ['hi', 'james', '!'],
        fns: [flow.writeString, flow.readString]
      },
      {
        name: 'empty',
        arr: [],
        fns: [flow.writeLong, flow.readLong],
      },
      {
        name: 'array of long',
        arr: [[3, 4], [], [1, 2]],
        fns: [
          function (arr) { flow.writeArray(arr, flow.writeLong); },
          function () { return flow.readArray(flow.readLong); }
        ]
      }
    ];

    tests.forEach(function (obj) {
      test(obj.name, function () {
        flow.offset = 0;
        flow.writeArray(obj.arr, obj.fns[0]);
        flow.offset = 0;
        assert.deepEqual(flow.readArray(obj.fns[1]), obj.arr);
      });
    });

  });

  suite('map', function () {

    var flow = newFlow(100);

    var tests = [
      {
        name: 'long',
        obj: {one: 1, foo: -1231},
        fns: [flow.writeLong, flow.readLong]
      },
      {
        name: 'string',
        obj: {foo: 'hi', 'foo.bar': 'james', '': '!'},
        fns: [flow.writeString, flow.readString]
      },
      {
        name: 'empty',
        obj: {},
        fns: [flow.writeLong, flow.readLong],
      },
      {
        name: 'array of long',
        obj: {one: [3, 4], two: [], three: [1, 2]},
        fns: [
          function (obj) { flow.writeArray(obj, flow.writeLong); },
          function () { return flow.readArray(flow.readLong); }
        ]
      }
    ];

    tests.forEach(function (obj) {
      test(obj.name, function () {
        flow.offset = 0;
        flow.writeMap(obj.obj, obj.fns[0]);
        flow.offset = 0;
        assert.deepEqual(flow.readMap(obj.fns[1]), obj.obj);
      });
    });

  });

});

function newFlow(n) {

  var buf = new Buffer(n);
  buf.fill(0);
  return new flow.Flow(buf);

}
