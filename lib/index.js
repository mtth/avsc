/* jshint node: true */

'use strict';

var streams = require('./streams'),
    types = require('./types'),
    fs = require('fs');


/**
 * Parse a schema and return the corresponding type.
 *
 * @param readerSchema {Object|String} Schema (type object or type name
 * string).
 * @param opts {Object} Parsing options. The following keys are currently
 * supported:
 *
 * + `namespace` Optional parent namespace.
 * + `registry` Optional registry of predefined type names.
 * + `unwrapUnions` By default, Avro expects all unions to be wrapped inside an
 *   object with a single key. Setting this to `true` will prevent this.
 *   (Defaults to `false`.)
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
 * @param path {String} Path to Avro schema file (stored in JSON format).
 * @param opts {Object} Parsing options. See `parse` above for details.
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
