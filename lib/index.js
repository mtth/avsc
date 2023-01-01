'use strict';

/**
 * Node.js entry point (see `etc/browser/` for browserify's entry points).
 *
 * It also adds Node.js specific functionality (for example a few convenience
 * functions to read Avro files from the local filesystem).
 */

let containers = require('./containers'),
    platform = require('./platform'),
    services = require('./services'),
    specs = require('./specs'),
    types = require('./types'),
    utils = require('./utils'),
    fs = require('fs');

/** Parse a schema and return the corresponding type or service. */
function parse(any, opts) {
  let schemaOrProtocol = specs.read(any);
  return schemaOrProtocol.protocol ?
    services.Service.forProtocol(schemaOrProtocol, opts) :
    types.Type.forSchema(schemaOrProtocol, opts);
}

const DECODER = new TextDecoder();

/** Extract a container file's header synchronously. */
function extractFileHeader(path, opts) {
  opts = opts || {};

  let decode = opts.decode === undefined ? true : !!opts.decode;
  let size = Math.max(opts.size || 4096, 4);
  let buf = Buffer.alloc(size);
  let tap = new utils.Tap(buf);
  let fd = fs.openSync(path, 'r');

  try {
    let pos = fs.readSync(fd, buf, 0, size);
    if (pos < 4 || !containers.MAGIC_BYTES.equals(buf.subarray(0, 4))) {
      return null;
    }

    let header = null;
    do {
      header = containers.HEADER_TYPE._read(tap);
    } while (!isValid());
    if (decode !== false) {
      let meta = header.meta;
      meta['avro.schema'] = JSON.parse(DECODER.decode(meta['avro.schema']));
      if (meta['avro.codec'] !== undefined) {
        meta['avro.codec'] = DECODER.decode(meta['avro.codec']);
      }
    }
    return header;
  } finally {
    fs.closeSync(fd);
  }

  function isValid() {
    if (tap.isValid()) {
      return true;
    }
    let len = 2 * tap.length;
    let buf = Buffer.alloc(len);
    len = fs.readSync(fd, buf, 0, len);
    tap.append(buf);
    return false;
  }
}

/** Readable stream of records from a local Avro file. */
function createFileDecoder(path, opts) {
  return fs.createReadStream(path)
    .pipe(new containers.streams.BlockDecoder(opts));
}

/** Writable stream of records to a local Avro file. */
function createFileEncoder(path, schema, opts) {
  let encoder = new containers.streams.BlockEncoder(schema, opts);
  encoder.pipe(fs.createWriteStream(path, {defaultEncoding: 'binary'}));
  return encoder;
}


module.exports = {
  Service: services.Service,
  Type: types.Type,
  assembleProtocol: specs.assembleProtocol,
  createFileDecoder,
  createFileEncoder,
  discoverProtocol: services.discoverProtocol,
  extractFileHeader,
  parse,
  readProtocol: specs.readProtocol,
  readSchema: specs.readSchema,
  streams: containers.streams,
  types: types.builtins,
  // Deprecated exports.
  Protocol: services.Service,
  assemble: platform.deprecate(
    specs.assembleProtocol,
    'use `assembleProtocol` instead'
  ),
  combine: platform.deprecate(
    types.Type.forTypes,
    'use `Type.forTypes` intead'
  ),
  infer: platform.deprecate(
    types.Type.forValue,
    'use `Type.forValue` instead'
  )
};
