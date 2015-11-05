/* jshint node: true */

'use strict';

/**
 * Main node.js entry point.
 *
 * See `etc/browser/avsc.js` for the entry point used for browserify.
 *
 */

var files = require('./files'),
    schemas = require('./schemas');


module.exports = {
  parse: files.parse,
  createFileDecoder: files.createFileDecoder,
  createFileEncoder: files.createFileEncoder,
  extractFileHeader: files.extractFileHeader,
  streams: files.streams,
  types: schemas.types
};
