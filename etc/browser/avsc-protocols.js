/* jshint browserify: true */

'use strict';

/**
 * Optional entry point for browser builds.
 *
 * To use it: `require('avsc/etc/browser/avsc-protocols.js')`.
 *
 */

var messages = require('../../lib/messages'),
    schemas = require('./lib/schemas'),
    types = require('../../lib/types');


function parse(schema, opts) {
  var obj = schemas.load(schema);
  return obj.protocol ?
    messages.createProtocol(obj, opts) :
    schemas.createType(obj, opts);
}


module.exports = {
  LogicalType: types.LogicalType,
  Protocol: messages.Protocol,
  Type: types.Type,
  parse: parse,
  types: types.builtins
};
