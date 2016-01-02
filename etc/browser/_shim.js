/* jshint browserify: true */

'use strict';

/**
 * Various shimming utilities for browserify.
 *
 * Since there are no utf8 and binary functions on browserify's `Buffer`, we
 * must also patch in tap methods using the generic slice and write methods.
 *
 */
var Tap = require('../../lib/utils').Tap;


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


/**
 * Similar to the one defined in `lib/files.js`, but without reading files.
 *
 */
function loadSchema(schema) {
  var obj;
  if (typeof schema == 'string') {
    try {
      obj = JSON.parse(schema);
    } catch (err) {
      // Nothing here (no filesystem access).
    }
  }
  if (obj === undefined) {
    obj = schema;
  }
  return obj;
}


module.exports = {
  loadSchema: loadSchema,
};
