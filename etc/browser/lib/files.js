/* jshint node: true */

'use strict';

/**
 * Shim without file-system operations.
 *
 * We also patch a few methods because of browser incompatibilities (see below
 * for more information).
 *
 */

// Since there are no utf8 and binary functions on browserify's `Buffer`, we
// must patch in tap methods using the generic slice and write methods.
(function polyfillTap() {
  var Tap = require('../../../lib/utils').Tap;

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
})();


/**
 * Schema loader, without file-system access.
 *
 */
function load(schema) {
  var obj;
  if (typeof schema == 'string' && schema !== 'null') {
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

/**
 * Default (erroring) hook for assembling IDLs.
 *
 */
function createImportHook() {
  return function (fpath, kind, cb) { cb(new Error('missing import hook')); };
}


module.exports = {
  createImportHook: createImportHook,
  load: load
};
