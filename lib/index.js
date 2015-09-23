/* jshint node: true */

'use strict';

// TODO: Add append option to `createWriteStream`.

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
 * @param path {String} Path to object container file or raw Avro file.
 * @param opts {Object} Options.
 *
 *  + `raw`
 *  + `writerType`, inferred if reading container file.
 *  + `readerType`, defaults to writer type.
 *  + `includeBuffer`, emit {obj, buf} instead of just decoded object. `buf`
 *    will contain the original bytes representation (the writer type's
 *    encoding).
 *
 */
function createReadStream(path, opts) {
  opts = opts || {};
  if (opts.raw === undefined) {
    opts.raw = !streams.Decoder.isContainerFile(path);
  }
  var Decoder = opts.raw ? streams.RawDecoder : streams.Decoder;
  return fs.createReadStream(path).pipe(new Decoder(opts));
}

/**
 * Convenience method to encode Avro objects.
 *
 * @param path {String} Path to object container file or raw Avro file.
 * @param opts {Object} Options.
 *
 *  + `raw`
 *  + `writerType`, inferred if piping records or from a decoder stream (from
 *    the reader type).
 *
 */
function createWriteStream(path, opts) {
  opts = opts || {};
  var Encoder = opts.raw ? streams.RawEncoder : streams.Encoder;
  var fileOpts = {defaultEncoding: 'binary'};
  return new Encoder(opts).pipe(fs.createWriteStream(path, fileOpts));
}


module.exports = {
  parse: parse,
  parseFile: parseFile,
  createReadStream: createReadStream,
  createWriteStream: createWriteStream,
  streams: streams,
  types: types
};
