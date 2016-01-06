/* jshint node: true */

'use strict';

/**
 * Main browserify entry point.
 *
 */

var containers = require('../../lib/containers'),
    protocols = require('../../lib/protocols'),
    schemas = require('./lib/schemas'),
    types = require('../../lib/types');


function parse(schema, opts) {
  var attrs = schemas.load(schema);
  return attrs.protocol ?
    protocols.createProtocol(attrs, opts) :
    types.createType(attrs, opts);
}


module.exports = {
  LogicalType: types.LogicalType,
  Protocol: protocols.Protocol,
  Type: types.Type,
  parse: parse,
  streams: containers.streams,
  types: types.builtins
};
