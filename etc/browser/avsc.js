/* jshint browserify: true */

'use strict';

/**
 * Shim entry point for browserify.
 *
 * It only exposes the part of the API which can run in the browser.
 *
 */

var types = require('../../lib/types'),
    Tap = require('../../lib/tap');


// No filesystem access in the browser.

function parse(schema, opts) {
  var obj;
  if (typeof schema == 'string') {
    try {
      obj = JSON.parse(schema);
    } catch (err) {
      // Pass. We don't support reading files here.
    }
  }
  if (obj === undefined) {
    obj = schema;
  }
  return types.Type.fromSchema(obj, opts);
}


// No utf8 and binary functions on browserify's `Buffer`, we must use the
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
  types: types
};
