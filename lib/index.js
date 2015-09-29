/* jshint node: true */

'use strict';


var streams = require('./streams'),
    types = require('./types'),
    fs = require('fs');


/**
 * Parse a schema and return the corresponding type.
 *
 * @param readerSchema {Object|String} Schema (type object or path to an Avro
 * schema file).
 * @param opts {Object} Parsing options. The following keys are currently
 * supported:
 *
 *  + `namespace` Optional parent namespace.
 *  + `registry` Optional registry of predefined type names.
 *  + `unwrapUnions` By default, Avro expects all unions to be wrapped inside
 *    an object with a single key. Setting this to `true` will prevent this.
 *    (Defaults to `false`.)
 *
 */
function parse(schema, opts) {
  if (schema instanceof types.Type) {
    return schema;
  }
  if (typeof schema == 'string') {
    // This is a file path.
    schema = JSON.parse(fs.readFileSync(schema));
  }
  return types.Type.fromSchema(schema, opts);
}

/**
 * Convenience method to decode an Avro container file.
 *
 * @param path {String} Path to object container file or raw Avro file.
 * @param opts {Object}
 *
 */
function decodeFile(path, opts) {
  return fs.createReadStream(path).pipe(new streams.BlockDecoder(opts));
}


module.exports = {
  parse: parse,
  decodeFile: decodeFile,
  getFileHeader: streams.BlockDecoder.getHeader,
  streams: streams,
  types: types
};
