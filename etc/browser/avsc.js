/* jshint node: true */

'use strict';

/**
 * Main browserify entry point.
 *
 */

var containers = require('../../lib/containers'),
    files = require('./lib/files'),
    protocols = require('../../lib/protocols'),
    schemas = require('../../lib/schemas'),
    types = require('../../lib/types');


function parse(schema, opts) {
  var attrs = files.load(schema);
  return attrs.protocol ?
    protocols.createProtocol(attrs, opts) :
    types.createType(attrs, opts);
}


module.exports = {
  Protocol: protocols.Protocol,
  Type: types.Type,
  assemble: schemas.assemble,
  parse: parse,
  streams: {
    BlockDecoder: containers.streams.BlockDecoder,
    BlockEncoder: containers.streams.BlockEncoder,
    FrameDecoder: protocols.streams.FrameDecoder,
    FrameEncoder: protocols.streams.FrameEncoder,
    RawDecoder: containers.streams.RawDecoder,
    RawEncoder: containers.streams.RawEncoder
  },
  types: types.builtins
};
