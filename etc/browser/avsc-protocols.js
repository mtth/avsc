/* jshint browserify: true */

'use strict';

/**
 * Optional entry point for browser builds.
 *
 * To use it: `require('avsc/etc/browser/avsc-protocols.js')`.
 *
 */

var protocols = require('../../lib/protocols'),
    schemas = require('./lib/schemas'),
    types = require('../../lib/types');


function parse(schema, opts) {
  var obj = schemas.load(schema);
  return obj.protocol ?
    protocols.createProtocol(obj, opts) :
    types.createType(obj, opts);
}


module.exports = {
  LogicalType: types.LogicalType,
  Protocol: protocols.Protocol,
  Type: types.Type,
  parse: parse,
  types: types.builtins
};
