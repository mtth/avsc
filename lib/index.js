/* jshint node: true */

'use strict';

var parse = require('./parse'),
    file = require('./file'),
    fs = require('fs');

/**
 * Convenience function to parse an Avro schema file (`.avsc` typically).
 *
 * @param path {String} Path to file.
 * @param opts {Object} Parsing options. See `parse` for details.
 *
 */
function parseFile(path, opts) {

  return parse.parse(JSON.parse(fs.readFileSync(path)), opts);

}

module.exports = {
  parse: parse.parse,
  parseFile: parseFile,
  getDecodeStream: file.getDecodeStream,
  getEncodeStream: file.getEncodeStream,
  streams: file.streams,
  types: parse.types
};
