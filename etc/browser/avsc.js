/* jshint browserify: true */

'use strict';

/**
 * Shim entry point for browserify.
 *
 * It only exposes the part of the API which can run in the browser.
 *
 */

var types = require('../../lib/types');


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


module.exports = {
  parse: parse,
  types: types
};
