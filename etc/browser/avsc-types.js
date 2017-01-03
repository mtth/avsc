/* jshint browserify: true */

'use strict';

/**
 * Optional entry point for browser builds.
 *
 * To use it: `require('avsc/etc/browser/avsc-types')`.
 */

var types = require('../../lib/types');


// Since there are no utf8 and binary functions on browserify's `Buffer`, we
// must patch in tap methods using the generic slice and write methods.
(function polyfillTap() {
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

  Tap.prototype.writeBinary = function (s, len) {
    var pos = this.pos;
    this.pos += len;
    if (this.pos > this.buf.length) {
      return;
    }
    this.buf.write(s, pos, len, 'binary');
  };
})();

/** Basic parse method, only supporting JSON parsing. */
function parse(any, opts) {
  var schema;
  if (typeof any == 'string') {
    try {
      schema = JSON.parse(any);
    } catch (err) {
      schema = any;
    }
  } else {
    schema = any;
  }
  return types.Type.forSchema(schema, opts);
}


module.exports = {
  Type: types.Type,
  parse: parse,
  types: types.builtins,
  // Deprecated exports (not using `util.deprecate` since it causes stack
  // overflow errors in the browser).
  combine: types.Type.forTypes,
  infer: types.Type.forValue
};
