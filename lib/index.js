/* jshint node: true */

'use strict';

/**
 * Node.js entry point (see `etc/browser/` for browserify's entry points).
 *
 * It also adds Node.js specific functionality (for example a few convenience
 * functions to read Avro files from the local filesystem).
 *
 */

var containers = require('./containers'),
    protocols = require('./protocols'),
    schemas = require('./schemas'),
    types = require('./types'),
    utils = require('./utils'),
    values = require('./values'),
    fs = require('fs'),
    util = require('util');


/**
 * Parse a schema and return the corresponding type or protocol.
 *
 */
function parse(any, opts) {
  var schema = schemas.parseSchema(any);
  return schema.protocol ?
    protocols.Protocol.forSchema(schema, opts) :
    types.Type.forSchema(schema, opts);
}

/**
 * Extract a container file's header synchronously.
 *
 */
function extractFileHeader(path, opts) {
  opts = opts || {};

  var decode = opts.decode === undefined ? true : !!opts.decode;
  var size = Math.max(opts.size || 4096, 4);
  var fd = fs.openSync(path, 'r');
  var buf = new Buffer(size);
  var pos = 0;
  var tap = new utils.Tap(buf);
  var header = null;

  while (pos < 4) {
    // Make sure we have enough to check the magic bytes.
    pos += fs.readSync(fd, buf, pos, size - pos);
  }
  if (containers.MAGIC_BYTES.equals(buf.slice(0, 4))) {
    do {
      header = containers.HEADER_TYPE._read(tap);
    } while (!isValid());
    if (decode !== false) {
      var meta = header.meta;
      meta['avro.schema'] = JSON.parse(meta['avro.schema'].toString());
      if (meta['avro.codec'] !== undefined) {
        meta['avro.codec'] = meta['avro.codec'].toString();
      }
    }
  }
  fs.closeSync(fd);
  return header;

  function isValid() {
    if (tap.isValid()) {
      return true;
    }
    var len = 2 * tap.buf.length;
    var buf = new Buffer(len);
    len = fs.readSync(fd, buf, 0, len);
    tap.buf = Buffer.concat([tap.buf, buf]);
    tap.pos = 0;
    return false;
  }
}

/**
 * Readable stream of records from a local Avro file.
 *
 */
function createFileDecoder(path, opts) {
  return fs.createReadStream(path)
    .pipe(new containers.streams.BlockDecoder(opts));
}

/**
 * Writable stream of records to a local Avro file.
 *
 */
function createFileEncoder(path, schema, opts) {
  var encoder = new containers.streams.BlockEncoder(schema, opts);
  encoder.pipe(fs.createWriteStream(path, {defaultEncoding: 'binary'}));
  return encoder;
}


module.exports = {
  Protocol: protocols.Protocol,
  Type: types.Type,
  assembleProtocolSchema: schemas.assembleProtocolSchema,
  createFileDecoder: createFileDecoder,
  createFileEncoder: createFileEncoder,
  discoverProtocolSchema: protocols.discoverProtocolSchema,
  extractFileHeader: extractFileHeader,
  parse: parse,
  parseProtocolSchema: schemas.parseProtocolSchema,
  parseTypeSchema: schemas.parseTypeSchema,
  streams: containers.streams,
  types: types.builtins,
  // Deprecated.
  assemble: util.deprecate(
    schemas.assembleProtocolSchema,
    'use `assembleProtocolSchema` instead'
  ),
  combine: util.deprecate(
    values.combine,
    'use `Type.combine` intead'
  ),
  infer: util.deprecate(
    values.infer,
    'use `Type.infer` instead'
  )
};
