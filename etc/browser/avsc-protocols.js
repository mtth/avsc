/* jshint browserify: true */

'use strict';

/**
 * Optional entry point for browser builds.
 *
 * To use it: `require('avsc/etc/browser/avsc-protocols')`.
 */

var avroTypes = require('./avsc-types'),
    protocols = require('../../lib/protocols'),
    schemas = require('../../lib/schemas'),
    utils = require('../../lib/utils');


/** Slightly enhanced parsing, supporting IDL declarations. */
function parse(any, opts) {
  var schema = schemas.parseSchema(any);
  return schema.protocol ?
    protocols.Protocol.forSchema(schema, opts) :
    avroTypes.Type.forSchema(schema, opts);
}


module.exports = {
  Protocol: protocols.Protocol,
  assembleProtocolSchema: schemas.assembleProtocolSchema,
  discoverProtocolSchema: protocols.discoverProtocolSchema,
  parse: parse,
  parseProtocolSchema: schemas.parseProtocolSchema,
  parseTypeSchema: schemas.parseTypeSchema,
  // Deprecated exports.
  assemble: schemas.assembleProtocolSchema,
};

utils.copyOwnProperties(avroTypes, module.exports);
