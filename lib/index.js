/* jshint node: true */

'use strict';


var Tap = require('./tap'),
    streams = require('./streams'),
    types = require('./types'),
    fs = require('fs');


/**
 * Parse a schema and return the corresponding type.
 *
 */
function parse(schema, opts) {
  var obj;
  if (typeof schema == 'string') {
    try {
      obj = JSON.parse(schema);
    } catch (err) {
      if (~schema.indexOf('/')) {
        // This can't be a valid name, so we interpret is as a filepath. This
        // makes is always feasible to read a file, independent of its name
        // (i.e. even if its name is valid JSON), by prefixing it with `./`.
        obj = JSON.parse(fs.readFileSync(schema));
      }
    }
  }
  if (obj === undefined) {
    obj = schema;
  }
  return types.Type.fromSchema(obj, opts);
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
  var tap = new Tap(buf);
  var header = null;

  while (pos < 4) {
    // Make sure we have enough to check the magic bytes.
    pos += fs.readSync(fd, buf, pos, size - pos);
  }
  if (streams.MAGIC_BYTES.equals(buf.slice(0, 4))) {
    do {
      header = streams.HEADER_TYPE._read(tap);
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
 * Stream of decoded records from a local Avro file.
 *
 */
function createFileDecoder(path, type, opts) {
  var input = fs.createReadStream(path);
  if (extractFileHeader(path)) {
    // Block file.
    return input.pipe(new streams.streams.BlockDecoder(opts))
      .on('metadata', function (writerType) {
        if (type && type.toString() !== writerType.toString()) {
          this.emit('error', 'type mismatch');
        }
      });
  } else {
    // Raw file.
    return input.pipe(new streams.streams.RawDecoder(type, opts));
  }
}


module.exports = {
  parse: parse,
  createFileDecoder: createFileDecoder,
  extractFileHeader: extractFileHeader,
  streams: streams.streams,
  types: types
};
