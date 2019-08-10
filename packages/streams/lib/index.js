/* jshint node: true */

'use strict';

var containers = require('./containers'),
    utils = require('./utils'),
    fs = require('fs');

var streams = containers.streams;

/** Extract a container file's header synchronously. */
function extractFileHeader(path, opts) {
  opts = opts || {};

  var decode = opts.decode === undefined ? true : !!opts.decode;
  var size = Math.max(opts.size || 4096, 4);
  var buf = utils.newBuffer(size);
  var fd = fs.openSync(path, 'r');

  try {
    var pos = fs.readSync(fd, buf, 0, size);
    if (pos < 4 || !containers.MAGIC_BYTES.equals(buf.slice(0, 4))) {
      return null;
    }

    var tap = new containers.Tap(buf);
    var header = null;
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
    return header;
  } finally {
    fs.closeSync(fd);
  }

  function isValid() {
    if (tap.isValid()) {
      return true;
    }
    var len = 2 * tap.buf.length;
    var buf = utils.newBuffer(len);
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
  var encoder = new containers.streams.BlockEncoder(schema, opts);
  encoder.pipe(fs.createWriteStream(path, {defaultEncoding: 'binary'}));
  return encoder;
}

module.exports = {
  BlockDecoder: streams.BlockDecoder,
  BlockEncoder: streams.BlockEncoder,
  RawDecoder: streams.RawDecoder,
  RawEncoder: streams.RawEncoder,
  extractFileHeader: extractFileHeader,
  createFileDecoder: createFileDecoder,
  createFileEncoder: createFileEncoder
};
