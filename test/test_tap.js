/* jshint node: true, mocha: true */

'use strict';


var Tap = require('../lib/tap'),
    assert = require('assert');


suite('tap', function () {

  suite('int & long', function () {

    testWriterReader({
      elems: [0, -1, 109213, -1211, -1312411211, 900719925474090],
      reader: function () { return this.readLong(); },
      skipper: function () { this.skipLong(); },
      writer: function (n) { this.writeLong(n); }
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

  suite('boolean', function () {

    testWriterReader({
      elems: [true, false],
      reader: function () { return this.readBoolean(); },
      skipper: function () { this.skipBoolean(); },
      writer: function (b) { this.writeBoolean(b); }
    });

  });

  suite('float', function () {

    testWriterReader({
      elems: [1, 3,1, -5, 1e9],
      reader: function () { return this.readFloat(); },
      skipper: function () { this.skipFloat(); },
      writer: function (b) { this.writeFloat(b); }
    });

  });

  suite('double', function () {

    testWriterReader({
      elems: [1, 3,1, -5, 1e12],
      reader: function () { return this.readDouble(); },
      skipper: function () { this.skipDouble(); },
      writer: function (b) { this.writeDouble(b); }
    });

  });

  suite('string', function () {

    testWriterReader({
      elems: ['ahierw', '', 'alh hewlii! rew'],
      reader: function () { return this.readString(); },
      skipper: function () { this.skipString(); },
      writer: function (s) { this.writeString(s); }
    });

  });

  suite('bytes', function () {

    testWriterReader({
      elems: [new Buffer('abc'), new Buffer(0), new Buffer([1, 5, 255])],
      reader: function () { return this.readBytes(); },
      skipper: function () { this.skipBytes(); },
      writer: function (b) { this.writeBytes(b); }
    });

  });

  suite('fixed', function () {

    testWriterReader({
      elems: [new Buffer([1, 5, 255])],
      reader: function () { return this.readFixed(3); },
      skipper: function () { this.skipFixed(3); },
      writer: function (b) { this.writeFixed(b, 3); }
    });

  });

  suite('binary', function () {

    test('write valid', function () {
      var tap = newTap(3);
      var s = '\x01\x02';
      tap.writeBinary(s, 2);
      assert.deepEqual(tap.buf, new Buffer([1,2,0]));
    });

    test('write invalid', function () {
      var tap = newTap(1);
      var s = '\x01\x02';
      tap.writeBinary(s, 2);
      assert.deepEqual(tap.buf, new Buffer([0]));
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
  var skipFn = opts.skipper;
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

  test('skip ' + name, function () {
    var tap = newTap(size || 1024);
    var i, l, elem, pos;
    for (i = 0, l = elems.length; i < l; i++) {
      tap.buf.fill(0);
      tap.pos = 0;
      elem = elems[i];
      writeFn.call(tap, elem);
      pos = tap.pos;
      tap.pos = 0;
      skipFn.call(tap, elem);
      assert.equal(tap.pos, pos);
    }
  });

}
