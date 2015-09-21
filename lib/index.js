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
 * @param writerSchema {Object|String} Optional writer schema. If specified,
 * the returned type will be particularized to read records written by that
 * writer schema.
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
function parse(readerSchema, writerSchema, opts) {

  if (opts === undefined) {
    opts = writerSchema;
    writerSchema = undefined;
  }

  var readerType;
  if (readerSchema instanceof types.Type) {
    readerType = readerSchema;
  } else {
    readerType = types.Type.fromSchema(readerSchema, opts);
  }

  var writerType;
  if (writerSchema) {
    if (writerSchema instanceof types.Type) {
      writerType = writerSchema;
    } else {
      writerType = types.Types.fromSchema(writerSchema, opts);
    }
    readerType = readerType.asReaderOf(writerType);
  }

  return readerType;

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
