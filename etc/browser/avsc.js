/* jshint browserify: true */

'use strict';

/**
 * Shim entry point used when `avsc` is `require`d from browserify.
 *
 * It doesn't expose any of the filesystem methods and patches a few others.
 *
 */

var messages = require('../../lib/messages'),
    schemas = require('../../lib/schemas'),
    shims = require('./_shims');


function parse(schema, opts) {
  var obj = shims.loadSchema(schema);
  return obj.protocol ?
    messages.createProtocol(obj, opts) :
    schemas.createType(obj, opts);
}


module.exports = {
  LogicalType: schemas.LogicalType,
  Protocol: messages.Protocol,
  Type: schemas.Type,
  parse: parse,
  types: schemas.types
};
