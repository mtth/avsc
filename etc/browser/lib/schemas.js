/* jshint node: true */

'use strict';

/**
 * Shim disabling file-system operations.
 *
 */

var Tap = require('../../../lib/utils').Tap;


// Since there are no utf8 and binary functions on browserify's `Buffer`, we
// must also patch in tap methods using the generic slice and write methods.

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


function load(schema) {
  var obj;
  if (typeof schema == 'string') {
    try {
      obj = JSON.parse(schema);
    } catch (err) {
      // No file loading here.
    }
  }
  if (obj === undefined) {
    obj = schema;
  }
  return obj;
}


module.exports = {
  load: load
};
