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
    files = require('./files'),
    protocols = require('./protocols'),
    schemas = require('./schemas'),
    types = require('./types'),
    utils = require('./utils'),
    values = require('./values'),
    fs = require('fs');


/**
 * Parse a schema and return the corresponding type or protocol.
 *
 */
function parse(schema, opts) {
  var attrs = files.load(schema);
  return attrs.protocol ?
    protocols.createProtocol(attrs, opts) :
    types.createType(attrs, opts);
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
  assemble: schemas.assemble,
  combine: values.combine,
  createFileDecoder: createFileDecoder,
  createFileEncoder: createFileEncoder,
  extractFileHeader: extractFileHeader,
  infer: values.infer,
  parse: parse,
  streams: containers.streams,
  types: types.builtins
};
