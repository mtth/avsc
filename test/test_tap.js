/* jshint node: true, mocha: true */

'use strict';

var Tap = require('../lib/tap'),
    assert = require('assert');


suite('tap', function () {

  suite('int & long', function () {

    testWriterReader({
      elems: [0, -1, 109213, -1211],
      writer: function (n) { this.writeLong(n); },
      reader: function () { return this.readLong(); }
    });

    test('write', function () {

      var tap = newTap(6);
      tap.writeLong(1440756011948);
      var buf = new Buffer(['0xd8', '0xce', '0x80', '0xbc', '0xee', '0x53']);
      assert(tap.isValid());
      assert(buf.equals(tap.buf));

    });

    test('read', function () {

      var buf = new Buffer(['0xd8', '0xce', '0x80', '0xbc', '0xee', '0x53']);
      assert.equal((new Tap(buf)).readLong(), 1440756011948);

    });

  });

  suite('null', function () {

    test('write read', function () {

      var tap = newTap(10);
      assert.strictEqual(tap.readNull(), null);

    });

  });

  suite('boolean', function () {

    testWriterReader({
      elems: [true, false],
      writer: function (b) { this.writeBoolean(b); },
      reader: function () { return this.readBoolean(); }
    });

  });

  suite('float', function () {

    testWriterReader({
      elems: [1, 3,1, -5, 1e9],
      writer: function (b) { this.writeFloat(b); },
      reader: function () { return this.readFloat(); }
    });

  });

  suite('double', function () {

    testWriterReader({
      elems: [1, 3,1, -5, 1e12],
      writer: function (b) { this.writeDouble(b); },
      reader: function () { return this.readDouble(); }
    });

  });

  suite('string', function () {

    testWriterReader({
      elems: ['ahierw', '', 'alh hewlii! rew'],
      writer: function (s) { this.writeString(s); },
      reader: function () { return this.readString(); }
    });

  });

  suite('bytes', function () {

    testWriterReader({
      elems: [new Buffer('abc'), new Buffer(0), new Buffer([1, 5, 255])],
      writer: function (b) { this.writeBytes(b); },
      reader: function () { return this.readBytes(); }
    });

  });

  suite('fixed', function () {

    testWriterReader({
      elems: [new Buffer([1, 5, 255])],
      writer: function (b) { this.writeFixed(b, 3); },
      reader: function () { return this.readFixed(3); }
    });

  });

  suite('array', function () {

    testWriterReader({
      name: 'long',
      elems: [[1, 49210914, -12391023, 0], [], [3]],
      writer: function (arr) { this.writeArray(arr, this.writeLong); },
      reader: function () { return this.readArray(this.readLong); }
    });

    testWriterReader({
      name: 'string',
      elems: [['hello'], [], ['hi', 'qwe']],
      writer: function (arr) { this.writeArray(arr, this.writeString); },
      reader: function () { return this.readArray(this.readString); }
    });

    testWriterReader({
      name: 'array long',
      elems: [[[], [1]], [], [[1,3]]],
      writer: function (arr) {
        this.writeArray(arr, function (ns) {
          this.writeArray(ns, this.writeLong);
        });
      },
      reader: function () {
        return this.readArray(function () {
          return this.readArray(this.readLong);
        });
      }
    });

  });

  suite('map', function () {

    testWriterReader({
      name: 'long',
      elems: [{one: 1, two: 2}, {}, {a: 4}],
      writer: function (arr) { this.writeMap(arr, this.writeLong); },
      reader: function () { return this.readMap(this.readLong); }
    });

    testWriterReader({
      name: 'array string',
      elems: [{a: ['a'], b: []}, {a: ['a', 'b']}, {}],
      writer: function (arr) {
        this.writeMap(arr, function (ns) {
          this.writeMap(ns, this.writeString);
        });
      },
      reader: function () {
        return this.readMap(function () {
          return this.readMap(this.readString);
        });
      }
    });

  });

});

function newTap(n) {

  var buf = new Buffer(n);
  buf.fill(0);
  return new Tap(buf);

}

function testWriterReader(opts) {

  var size = opts.size;
  var elems = opts.elems;
  var writeFn = opts.writer;
  var readFn = opts.reader;
  var name = opts.name || '';

  test('write read ' + name, function () {
    var tap = newTap(size || 1024);
    var i, l, elem;
    for (i = 0, l = elems.length; i < l; i++) {
      tap.buf.fill(0);
      tap.pos = 0;
      elem = elems[i];
      writeFn.call(tap, elem);
      tap.pos = 0;
      assert.deepEqual(readFn.call(tap), elem);
    }
  });

  test('read over ' + name, function () {
    var tap = new Tap(new Buffer(0));
    readFn.call(tap); // Shouldn't throw.
    assert(!tap.isValid());
  });

  test('write over ' + name, function () {
    var tap = new Tap(new Buffer(0));
    writeFn.call(tap, elems[0]); // Shouldn't throw.
    assert(!tap.isValid());
  });

}
