/* jshint browserify: true */

'use strict';

/**
 * Entry point for browserify.
 *
 */

var types = require('../../lib/types');


/**
 * Refer to the original function for documentation.
 *
 * Only difference is that we don't support reading from files here.
 *
 */
function parse(schema, opts) {
  var obj;
  if (typeof schema == 'string') {
    try {
      obj = JSON.parse(schema);
    } catch (err) {
      // Pass.
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
