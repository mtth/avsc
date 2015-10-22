/* jshint node: true */

'use strict';

/**
 * Script to help validate a custom long implementation.
 *
 * It expects the long implementation to be the exported object from the
 * module.
 *
 */

var avsc = require('../../lib'),
    assert = require('assert'),
    path = require('path');


var longPath = process.argv[2];
if (!longPath) {
  console.error('usage: node validate_custom_long.js PATH');
  process.exit(1);
}

// The custom long type.
var TYPE = require(path.resolve(longPath));

// The default one (used to encode small numbers).
var defaultLongType = avsc.parse('long');

// A few small numbers to start with.
var nums = [
  0, -1, 1, 2, -1023, 1024, Math.pow(2, 31) - 1, -Math.pow(2, 32),
  Math.pow(2, 47), -Math.pow(2, 48)
];
nums.forEach(function (n) {
  var buf = defaultLongType.toBuffer(n);
  assertSupported(buf, n);
});

// And a few chosen buffers of numbers larger than double integer precision.
var bufs = [
  new Buffer([0x86, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x20]),
  new Buffer([0x86, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x81, 0x21]),
  new Buffer([0x87, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x20]),
  new Buffer([0x89, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x90, 0xa3, 0x01])
];
bufs.forEach(assertSupported);

console.log('ok!');

function assertSupported(buf, n) {
  n = n || buf.inspect();
  var l = TYPE.fromBuffer(buf);
  assert(TYPE.isValid(l), 'should be valid: ' + n);
  assert.deepEqual(TYPE.toBuffer(l), buf, 'should roundtrip: ' + n);
}
