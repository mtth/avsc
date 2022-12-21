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
    buffer = require('buffer'),
    fs = require('fs');


let Buffer = buffer.Buffer;

/** Parse a schema and return the corresponding type or service. */
function parse(any, opts) {
  let schemaOrProtocol = specs.read(any);
  return schemaOrProtocol.protocol ?
    services.Service.forProtocol(schemaOrProtocol, opts) :
    types.Type.forSchema(schemaOrProtocol, opts);
}

/** Extract a container file's header synchronously. */
function extractFileHeader(path, opts) {
  opts = opts || {};

  let decode = opts.decode === undefined ? true : !!opts.decode;
  let size = Math.max(opts.size || 4096, 4);
  let buf = utils.newBuffer(size);
  let tap = new utils.Tap(buf);
  let fd = fs.openSync(path, 'r');

  try {
    let pos = fs.readSync(fd, buf, 0, size);
    if (pos < 4 || !containers.MAGIC_BYTES.equals(buf.slice(0, 4))) {
      return null;
    }

    let header = null;
    do {
      header = containers.HEADER_TYPE._read(tap);
    } while (!isValid());
    if (decode !== false) {
      let meta = header.meta;
      meta['avro.schema'] = JSON.parse(meta['avro.schema'].toString());
      if (meta['avro.codec'] !== undefined) {
        meta['avro.codec'] = meta['avro.codec'].toString();
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
    let len = 2 * tap.buf.length;
    let buf = utils.newBuffer(len);
    len = fs.readSync(fd, buf, 0, len);
    tap.buf = Buffer.concat([tap.buf, buf]);
    tap.pos = 0;
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
  createFileDecoder: createFileDecoder,
  createFileEncoder: createFileEncoder,
  discoverProtocol: services.discoverProtocol,
  extractFileHeader: extractFileHeader,
  parse: parse,
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
