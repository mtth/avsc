/* jshint browserify: true */

'use strict';

/**
 * Optional entry point for browser builds.
 *
 * To use it: `require('avsc/etc/browser/avsc-types.js')`.
 *
 */

var types = require('../../lib/types'),
    schemas = require('./lib/schemas');


function parse(schema, opts) {
  return types.createType(schemas.load(schema), opts);
}


module.exports = {
  LogicalType: types.LogicalType,
  Type: types.Type,
  parse: parse,
  types: types.builtins
};
