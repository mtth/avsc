/* jshint node: true */

// TODO: Add snappy support. Or maybe support for custom decompressors.
// TODO: Get `Decoder` to work even for decompressor that don't yield to the
// event loop.

'use strict';


var types = require('./types'),
    Tap = require('./tap'),
    utils = require('./utils'),
    fs = require('fs'),
    stream = require('stream'),
    util = require('util'),
    zlib = require('zlib');


// Type of Avro header.
var HEADER_TYPE = types.Type.fromSchema({
  type: 'record',
  name: 'org.apache.avro.file.Header',
  fields : [
    {name: 'magic', type: {type: 'fixed', name: 'Magic', size: 4}},
    {name: 'meta', type: {type: 'map', values: 'bytes'}},
    {name: 'sync', type: {type: 'fixed', name: 'Sync', size: 16}}
  ]
});

// Type of each block.
var BLOCK_TYPE = types.Type.fromSchema({
  type: 'record',
  name: 'org.apache.avro.file.Block',
  fields : [
    {name: 'count', type: 'long'},
    {name: 'data', type: 'bytes'},
    {name: 'sync', type: {type: 'fixed', name: 'Sync', size: 16}}
  ]
});

// First 4 bytes of an Avro object container file.
var MAGIC_BYTES = new Buffer('Obj\x01');

// Convenience import.
var AvscError = utils.AvscError;


/**
 * Duplex stream for decoding fragments.
 *
 * @param opts {Object}
 *
 *  + writerType
 *  + readerType
 *
 */
function RawDecoder(opts) {
  opts = opts || {};

  stream.Transform.call(this, {readableObjectMode: true});

  this.readerType = opts.readerType;
  this.writerType = opts.writerType;
  this._readOjb = null;
  this._tap = new Tap(new Buffer(0));
}
util.inherits(RawDecoder, stream.Transform);

RawDecoder.prototype._transform = function (chunk, encoding, cb) {
  if (!this.writerType) {
    this.emit('error', new AvscError('no writer type set'));
    return;
  }

  try {
    if (this.readerType) {
      this._readObj = this.readerType.createAdapter(this.writerType)._read;
    } else {
      this._readObj = this.writerType._read;
    }
  } catch (err) {
    this.emit('error', err);
    return;
  }

  this._transform = this._transformChunk;
  this._transform(chunk, encoding, cb);
};

RawDecoder.prototype._transformChunk = function (chunk, encoding, cb) {
  var tap = this._tap;
  tap.buf = Buffer.concat([tap.buf.slice(tap.pos), chunk]);
  tap.pos = 0;

  var len = tap.buf.length;
  while (tap.pos < len) {
    this.push(this._readObj.call(tap));
  }

  cb();
};


/**
 * Duplex stream for decoding object container files.
 *
 * @param opts {Object}
 *
 *  + readerType
 *  + unordered
 *
 */
function Decoder(opts) {
  opts = opts || {};

  stream.Duplex.call(this, {
    allowHalfOpen: true,
    readableObjectMode: true
  });

  this.readerType = opts.readerType || null;
  this.writerType = null;

  this._tap = new Tap(new Buffer(0));
  this._sync = null; // Sync marker.
  this._readObj = null;
  this._decompress = null; // Decompression function.
  this._queue = opts.unordered ? [] : new utils.OrderedQueue();
  this._index = 0; // Next block index.
  this._pending = 0; // Number of blocks undergoing decompression.
  this._needPush = false;
  this._finished = false;

  this.on('finish', function () { this._finished = true; });
}
util.inherits(Decoder, stream.Duplex);

Decoder.getHeader = function (path, decode) {
  var fd = fs.openSync(path, 'r');
  var buf = new Buffer(4096);
  var pos = 0;
  var tap = new Tap(buf);
  var header = null;

  while (pos < 4) {
    // Make sure we have enough to check the magic bytes.
    pos += fs.readSync(fd, buf, pos, 4096 - pos);
  }
  if (MAGIC_BYTES.equals(buf.slice(0, 4))) {
    do {
      header = HEADER_TYPE._read.call(tap);
    } while (!isValid());
    if (decode !== false) {
      var meta = header.meta;
      meta['avro.schema'] = JSON.parse(meta['avro.schema'].toString());
      meta['avro.codec'] = meta['avro.codec'].toString();
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
};

Decoder.prototype._decodeHeader = function () {
  var tap = this._tap;
  var header = HEADER_TYPE._read.call(tap);
  if (!tap.isValid()) {
    // Wait until more data arrives.
    return false;
  }

  if (!MAGIC_BYTES.equals(header.magic)) {
    this.emit('error', new AvscError('invalid magic bytes'));
    return;
  }

  var codec = header.meta['avro.codec'].toString();
  switch (codec) {
    case 'null':
      this._decompress = function (buf, cb) {
        // "Fake" async call.
        setTimeout(function () { cb(null, buf); }, 0);
      };
      break;
    case 'deflate':
      this._decompress = zlib.inflateRaw;
      break;
    case 'snappy':
      this.emit(new AvscError('snappy not yet supported'));
      return;
    default:
      this.emit(new AvscError('unknown codec: %s', codec));
      return;
  }

  try {
    var schema = JSON.parse(header.meta['avro.schema'].toString());
    this.writerType = types.Type.fromSchema(schema);
    if (this.readerType) {
      this._readObj = this.readerType.createAdapter(this.writerType)._read;
    } else {
      this._readObj = this.writerType._read;
    }
  } catch (err) {
    this.emit('error', err);
    return;
  }

  this._sync = header.sync;
  this.emit('metadata', schema, codec, this._sync);
  return true;
};

Decoder.prototype._write = function (chunk, encoding, cb) {
  var tap = this._tap;
  tap.buf = Buffer.concat([tap.buf, chunk]);
  tap.pos = 0;

  if (this._decodeHeader()) {
    // We got the header, switch to block decoding mode.
    this._write = this._writeChunk;
  }

  // Call it directly in case we already have all the data (in which case
  // `_write` wouldn't get called anymore).
  this._write(new Buffer(0), encoding, cb);
};

Decoder.prototype._writeChunk = function (chunk, encoding, cb) {
  var tap = this._tap;
  tap.buf = Buffer.concat([tap.buf.slice(tap.pos), chunk]);
  tap.pos = 0;

  var block;
  while ((block = getBlock())) {
    if (!this._sync.equals(block.sync)) {
      cb(new AvscError('invalid sync marker'));
      return; // TODO: Try to recover?
    }
    this._decompress(block.data, this._addBlockCallback());
  }

  cb();

  function getBlock() {
    var pos = tap.pos;
    var block = BLOCK_TYPE._read.call(tap);
    if (!tap.isValid()) {
      tap.pos = pos;
      return null;
    }
    return block;
  }
};

Decoder.prototype._addBlockCallback = function () {
  var self = this;
  var index = this._index++;
  this._pending++;

  return function (err, data) {
    if (err) {
      self.emit('error', err);
      return;
    }
    self._pending--;
    self._queue.push(new BlockData(index, data));
    if (self._needPush) {
      self._needPush = false;
      self._read();
    }
  };
};

Decoder.prototype._read = function () {
  var self = this;
  var blockData = this._queue.pop();
  if (!blockData) {
    if (this._finished && !this._pending) {
      process.nextTick(function () { self.push(null); });
    } else {
      this._needPush = true;
    }
    return;
  }

  var tap = new Tap(blockData.data);
  var len = tap.buf.length;
  while (tap.pos < len) {
    this.push(this._readObj.call(tap));
  }
};


/**
 * Duplex stream for encoding.
 *
 * @param opts {Object}
 *
 *  + writerType
 *  + batchSize
 *
 */
function RawEncoder(opts) {
  opts = opts || {};

  stream.Transform.call(this, {writableObjectMode: true});

  this.writerType = opts.writerType || null;
  this._tap = new Tap(new Buffer(opts.batchSize || 65536));
}
util.inherits(RawEncoder, stream.Transform);

RawEncoder.prototype._transform = function (obj, encoding, cb) {
  if (!this.writerType) {
    if (!(obj.$type instanceof types.Type)) {
      cb(new AvscError('unable to infer writer type'));
      return;
    }
    this.writerType = obj.$type;
  }

  this._writeObj = this.writerType._write;
  this._transform = RawEncoder.prototype._transformChunk;
  this._transform(obj, encoding, cb);
};

RawEncoder.prototype._transformChunk = function (obj, encoding, cb) {
  var tap = this._tap;
  var buf = tap.buf;
  var pos = tap.pos;

  this._writeObj.call(tap, obj);
  if (!tap.isValid()) {
    if (pos) {
      // Emit any valid data.
      this.push(tap.buf.slice(0, pos));
    }
    var len = tap.pos - pos;
    if (len > buf.length) {
      // Not enough space for last written object, need to resize.
      tap.buf = new Buffer(2 * len);
    }
    tap.pos = 0;
    this._writeObj.call(tap, obj); // Rewrite last failed write.
  }

  cb();
};

RawEncoder.prototype._flush = function (cb) {
  var tap = this._tap;
  var pos = tap.pos;
  if (pos) {
    // This should only ever be false if nothing is written to the stream.
    this.push(tap.buf.slice(0, pos));
  }
  cb();
};


/**
 * Duplex stream to write object container files.
 *
 * @param opts {Object}
 *
 *  + `blockSize`
 *  + `codec`
 *  + `omitHeader`, useful to append to an existing block file.
 *  + `writerType`
 *
 */
function Encoder(opts) {
  opts = opts || {};

  stream.Duplex.call(this, {
    allowHalfOpen: true,
    writableObjectMode: true
  });
}
util.inherits(Encoder, stream.Duplex);

Encoder.prototype._write = function (obj, encoding, cb) {
  // TODO.
  cb();
};

Encoder.prototype._read = function () {
  // TODO.
};


// Helpers.

/**
 * An indexed block.
 *
 * This is used since decompression can cause some some blocks to be returned
 * out of order.
 *
 */
function BlockData(index, data) {
  this.index = index;
  this.data = data;
}


module.exports = {
  Decoder: Decoder,
  RawDecoder: RawDecoder,
  Encoder: Encoder,
  RawEncoder: RawEncoder
};
