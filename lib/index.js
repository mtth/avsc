/* jshint node: true */

'use strict';

var streams = require('./streams'),
    types = require('./types'),
    fs = require('fs');


/**
 * Schema parsing with some caching.
 *
 */
function parse(schema, opts) {

  if (schema instanceof types.Type) {
    return schema;
  }

  return types.Type.fromSchema(schema, opts);

}

/**
 * Convenience function to parse an Avro schema file (`.avsc` typically).
 *
 * @param path {String} Path to file.
 * @param opts {Object} Parsing options. See `parse` for details.
 *
 */
function parseFile(path, opts) {

  return parse(JSON.parse(fs.readFileSync(path)), opts);

}

/**
 * Convenience method to decode a file.
 *
 * @param path {String} Path to container object file or fragments file.
 * @param opts {Object} Options.
 *
 */
function decodeFile(path, schema, opts) {

  return fs.createReadStream(path).pipe(new streams.Decoder(schema, opts));

}


module.exports = {
  parse: parse,
  parseFile: parseFile,
  decodeFile: decodeFile,
  streams: streams,
  types: types
};
