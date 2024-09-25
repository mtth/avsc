'use strict';

/**
 * Node.js entry point (see `etc/browser/` for browserify's entry points).
 *
 * It also adds Node.js specific functionality (for example a few convenience
 * functions to read Avro files from the local filesystem).
 */

let containers = require('./containers'),
    specs = require('./specs'),
    types = require('./types'),
    utils = require('./utils'),
    fs = require('fs');


const DECODER = new TextDecoder();

/** Extract a container file's header synchronously. */
function extractFileHeader(path, opts) {
  opts = opts || {};

  let decode = opts.decode === undefined ? true : !!opts.decode;
  let size = Math.max(opts.size || 4096, 4);
  let buf = new Uint8Array(size);
  let tap = new utils.Tap(buf);
  let fd = fs.openSync(path, 'r');

  try {
    let pos = fs.readSync(fd, buf, 0, size);
    if (
      pos < 4 ||
      !utils.bufEqual(containers.MAGIC_BYTES, buf.subarray(0, 4))
    ) {
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
    let buf = new Uint8Array(len);
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
  Type: types.Type,
  assembleProtocol: specs.assembleProtocol,
  createFileDecoder,
  createFileEncoder,
  extractFileHeader,
  readProtocol: specs.readProtocol,
  readSchema: specs.readSchema,
  streams: containers.streams,
  types: types.builtins,
};
