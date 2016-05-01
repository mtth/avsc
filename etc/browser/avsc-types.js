/* jshint browserify: true */

'use strict';

/**
 * Optional entry point for browser builds.
 *
 * To use it: `require('avsc/etc/browser/avsc-types')`.
 *
 */

var files = require('./lib/files'),
    types = require('../../lib/types'),
    values = require('../../lib/values');


function parse(schema, opts) {
  return types.createType(files.load(schema), opts);
}


module.exports = {
  Type: types.Type,
  combine: values.combine,
  infer: values.infer,
  parse: parse,
  types: types.builtins
};
