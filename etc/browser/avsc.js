/* jshint browserify: true */

'use strict';

/**
 * Shim entry point used when `avsc` is `require`d from browserify.
 *
 * It doesn't expose any of the filesystem methods and patches a few others.
 *
 */

var Tap = require('../../lib/utils').Tap,
    files = require('../../lib/files'),
    messages = require('../../lib/messages'),
    schemas = require('../../lib/schemas');

/* istanbul ignore next */
if (process.browser) {
  // No utf8 and binary functions on browserify's `Buffer`, we must patch in
  // the generic slice and write equivalents. We guard this by a check (true
  // when run from a browserify `require`), since this module is always
  // required by tests (see `test/mocha.opts`).

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

}


module.exports = {
  LogicalType: schemas.LogicalType,
  Protocol: messages.Protocol,
  Type: schemas.Type,
  parse: files.parse,
  streams: files.streams,
  types: schemas.types
};
