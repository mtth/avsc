/* jshint browserify: true */

'use strict';

/**
 * Shim entry point used when `avsc` is `require`d from browserify.
 *
 * It doesn't expose any of the filesystem methods and patches a few others.
 *
 */

var Tap = require('../../lib/utils').Tap,
    schemas = require('../../lib/schemas');


function parse(schema, opts) {
  var obj;
  if (typeof schema == 'string') {
    try {
      obj = JSON.parse(schema);
    } catch (err) {
      // Pass. No file reading from the browser.
    }
  }
  if (obj === undefined) {
    obj = schema;
  }
  return schemas.createType(obj, opts);
}

// No utf8 and binary functions on browserify's `Buffer`, we must patch in the
// generic slice and write equivalents.

Tap.prototype.readString = function () {
  var len = this.readLong();
  var pos = this.pos;
  var buf = this.buf;
  this.pos += len;
  if (this.pos > buf.length) {
    return;
  }
  return this.buf.slice(pos, pos + len).toString();
};

Tap.prototype.writeString = function (s) {
  var len = Buffer.byteLength(s);
  this.writeLong(len);
  var pos = this.pos;
  this.pos += len;
  if (this.pos > this.buf.length) {
    return;
  }
  this.buf.write(s, pos);
};

Tap.prototype.writeBinary = function (s, len) {
  var pos = this.pos;
  this.pos += len;
  if (this.pos > this.buf.length) {
    return;
  }
  this.buf.write(s, pos, len, 'binary');
};


module.exports = {
  parse: parse,
  types: schemas.types
};
