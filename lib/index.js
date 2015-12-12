/* jshint node: true */

'use strict';

/**
 * Main node.js entry point.
 *
 * See `etc/browser/avsc.js` for the entry point used for browserify.
 *
 */

var files = require('./files'),
    protocols = require('./protocols'),
    schemas = require('./schemas');


module.exports = {
  LogicalType: schemas.LogicalType,
  Protocol: protocols.Protocol,
  Type: schemas.Type,
  createFileDecoder: files.createFileDecoder,
  createFileEncoder: files.createFileEncoder,
  extractFileHeader: files.extractFileHeader,
  parse: files.parse,
  streams: files.streams,
  types: schemas.types
};
