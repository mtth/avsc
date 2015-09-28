/* jshint node: true */

// TODO: Add snappy support. Or maybe support for custom decompressors.

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

// Used to encode each block, without having to copy all its data.
var LONG_TYPE = types.Type.fromSchema('long');

// First 4 bytes of an Avro object container file.
var MAGIC_BYTES = new Buffer('Obj\x01');

// Convenience import.
var AvscError = utils.AvscError;


/**
 * Duplex stream for decoding fragments.
 *
 * @param opts {Object}
 *
 *  + type
 *  + decode
 *
 */
function RawDecoder(type, opts) {
  opts = opts || {};

  if (!(type instanceof types.Type)) {
    throw new AvscError('invalid type');
  }

  stream.Duplex.call(this, {readableObjectMode: true});
  var decode = opts.decode === undefined ? true : !!opts.decode;

  this._type = type;
  this._readObj = createReader(decode, this._type);
  this._tap = new Tap(new Buffer(0));
  this._needPush = false;

  this.on('finish', function () { this.push(null); });
}
util.inherits(RawDecoder, stream.Duplex);

RawDecoder.prototype._write = function (chunk, encoding, cb) {
  var tap = this._tap;
  tap.buf = Buffer.concat([tap.buf.slice(tap.pos), chunk]);
  tap.pos = 0;
  if (this._needPush) {
    this._needPush = false;
    this._read();
  }
  cb();
};

RawDecoder.prototype._read = function () {
  var tap = this._tap;
  var pos = tap.pos;
  var obj = this._readObj.call(tap);
  if (tap.isValid()) {
    this.push(obj);
  } else {
    tap.pos = pos;
    this._needPush = true;
  }
};


/**
 * Duplex stream for decoding object container files.
 *
 * @param opts {Object}
 *
 *  + parseOpts
 *  + decode
 *
 */
function BlockDecoder(opts) {
  opts = opts || {};

  stream.Duplex.call(this, {
    allowHalfOpen: true, // For async decompressors.
    readableObjectMode: true
  });

  this._type = null;
  this._parseOpts = opts.parseOpts || {};
  this._tap = new Tap(new Buffer(0));
  this._blockTap = new Tap(new Buffer(0));
  this._syncMarker = null;
  this._readObj = null;
  this._decode = opts.decode === undefined ? true : !!opts.decode;
  this._queue = new utils.OrderedQueue();
  this._decompress = null; // Decompression function.
  this._index = 0; // Next block index.
  this._pending = 0; // Number of blocks undergoing decompression.
  this._needPush = false;
  this._finished = false;

  this.on('finish', function () {
    this._finished = true;
    if (!this._pending) {
      this.push(null);
    }
  });
}
util.inherits(BlockDecoder, stream.Duplex);

BlockDecoder.getHeader = function (path, opts) {
  opts = opts || {};

  var decode = opts.decode === undefined ? true : !!opts.decode;
  var size = Math.max(opts.size || 4096, 4); // Mostly for testing.
  var fd = fs.openSync(path, 'r');
  var buf = new Buffer(size);
  var pos = 0;
  var tap = new Tap(buf);
  var header = null;

  while (pos < 4) {
    // Make sure we have enough to check the magic bytes.
    pos += fs.readSync(fd, buf, pos, size - pos);
  }
  if (MAGIC_BYTES.equals(buf.slice(0, 4))) {
    do {
      header = HEADER_TYPE._read.call(tap);
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
};

BlockDecoder.prototype._decodeHeader = function () {
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

  var codec = (header.meta['avro.codec'] || 'null').toString();
  switch (codec) {
    case 'null':
      this._decompress = function (buf, cb) { cb(null, buf); };
      break;
    case 'deflate':
      this._decompress = zlib.inflateRaw;
      break;
    default:
      this.emit('error', new AvscError('unknown codec: %s', codec));
      return;
  }

  try {
    var schema = JSON.parse(header.meta['avro.schema'].toString());
    this._type = types.Type.fromSchema(schema, this._parseOpts);
  } catch (err) {
    this.emit('error', err);
    return;
  }

  this._readObj = createReader(this._decode, this._type);
  this._syncMarker = header.sync;
  this.emit('metadata', this._type, codec, header);
  return true;
};

BlockDecoder.prototype._write = function (chunk, encoding, cb) {
  var tap = this._tap;
  tap.buf = Buffer.concat([tap.buf, chunk]);
  tap.pos = 0;

  if (!this._decodeHeader()) {
    process.nextTick(cb);
    return;
  }

  // We got the header, switch to block decoding mode. Also, call it directly
  // in case we already have all the data (in which case `_write` wouldn't get
  // called anymore).
  this._write = this._writeChunk;
  this._write(new Buffer(0), encoding, cb);
};

BlockDecoder.prototype._writeChunk = function (chunk, encoding, cb) {
  var tap = this._tap;
  tap.buf = Buffer.concat([tap.buf.slice(tap.pos), chunk]);
  tap.pos = 0;

  var block;
  while ((block = tryReadBlock(tap))) {
    if (!this._syncMarker.equals(block.sync)) {
      cb(new AvscError('invalid sync marker'));
      return;
    }
    this._decompress(block.data, this._createBlockCallback());
  }

  cb();
};

BlockDecoder.prototype._createBlockCallback = function () {
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

BlockDecoder.prototype._read = function () {
  var tap = this._blockTap;
  if (tap.pos >= tap.buf.length) {
    var data = this._queue.pop();
    if (!data) {
      if (this._finished && !this._pending) {
        this.push(null);
      } else {
        this._needPush = true;
      }
      return; // Wait for more data.
    }
    tap.buf = data.buf;
    tap.pos = 0;
  }

  this.push(this._readObj.call(tap)); // The read is guaranteed valid.
};


/**
 * Duplex stream for encoding.
 *
 * @param type
 * @param opts {Object}
 *
 *  + batchSize
 *  + unsafe
 *
 */
function RawEncoder(type, opts) {
  opts = opts || {};

  if (!(type instanceof types.Type)) {
    throw new AvscError('invalid type');
  }

  stream.Transform.call(this, {writableObjectMode: true});

  this._type = type;
  this._writeObj = type._write;
  this._tap = new Tap(new Buffer(opts.batchSize || 65536));
}
util.inherits(RawEncoder, stream.Transform);

RawEncoder.prototype._transform = function (obj, encoding, cb) {
  var tap = this._tap;
  var buf = tap.buf;
  var pos = tap.pos;

  if (!this._unsafe && !this._type.isValid(obj)) {
    this.emit('error', new AvscError('invalid object: %j', obj));
    return;
  }

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
 * @param type
 * @param opts {Object}
 *
 *  + `blockSize`
 *  + `codec`
 *  + `omitHeader`, useful to append to an existing block file.
 *  + `unsafe`
 *
 */
function BlockEncoder(type, opts) {
  opts = opts || {};

  if (!(type instanceof types.Type)) {
    throw new AvscError('invalid type');
  }

  stream.Duplex.call(this, {
    allowHalfOpen: true, // To support async compressors.
    writableObjectMode: true
  });

  this._type = type;
  this._writeObj = type._write;
  this._unsafe = opts.unsafe;
  this._blockSize = opts.blockSize || 65536;
  this._tap = new Tap(new Buffer(this._blockSize));
  this._codec = opts.codec || 'null';
  this._compress = null;
  this._omitHeader = opts.omitHeader || false;
  this._blockCount = 0;
  this._syncMarker = opts.syncMarker || new utils.Lcg().nextBuffer(16);
  this._queue = new utils.OrderedQueue();
  this._pending = 0;
  this._finished = false;
  this._needPush = false;

  this.on('finish', function () {
    this._finished = true;
    if (this._blockCount) {
      this._flushChunk();
    }
  });
}
util.inherits(BlockEncoder, stream.Duplex);

BlockEncoder.MAGIC_BYTES = MAGIC_BYTES;

BlockEncoder.HEADER_TYPE = HEADER_TYPE;

BlockEncoder.BLOCK_TYPE = BLOCK_TYPE;

BlockEncoder.prototype._write = function (obj, encoding, cb) {
  if (!this._omitHeader) {
    var meta = {
      'avro.schema': new Buffer(this._type.toString()),
      'avro.codec': new Buffer(this._codec)
    };
    var Header = HEADER_TYPE.getRecordConstructor();
    var header = new Header(MAGIC_BYTES, meta, this._syncMarker);
    this.push(header.$encode());
  }

  switch (this._codec) {
    case 'null':
      this._compress = function (buf, cb) {
        // "Fake" async call.
        setTimeout(function () { cb(null, buf); }, 0);
      };
      break;
    case 'deflate':
      this._compress = zlib.deflateRaw;
      break;
    default:
      this.emit('error', new AvscError('unsupported codec: %s', this._codec));
      return;
  }

  this._write = this._writeChunk;
  this._write(obj, encoding, cb);
};

BlockEncoder.prototype._writeChunk = function (obj, encoding, cb) {
  var tap = this._tap;
  var pos = tap.pos;

  if (!this._unsafe && !this._type.isValid(obj)) {
    this.emit('error', new AvscError('invalid object: %j', obj));
    return;
  }

  this._writeObj.call(tap, obj);
  if (!tap.isValid()) {
    if (pos) {
      this._flushChunk(pos);
    }
    var len = tap.pos - pos;
    if (len > this._blockSize) {
      // Not enough space for last written object, need to resize.
      this._blockSize *= 2;
    }
    tap.buf = new Buffer(this._blockSize); // TODO: Check if we can reuse.
    tap.pos = 0;
    this._writeObj.call(tap, obj); // Rewrite last failed write.
  }
  this._blockCount++;

  cb();
};

BlockEncoder.prototype._flushChunk = function (pos) {
  var tap = this._tap;
  pos = pos || tap.pos;
  this._compress(tap.buf.slice(0, pos), this._createBlockCallback());
  this._blockCount = 0;
};

BlockEncoder.prototype._read = function () {
  var self = this;
  var data = this._queue.pop();
  if (!data) {
    if (this._finished && !this._pending) {
      process.nextTick(function () { self.push(null); });
    } else {
      this._needPush = true;
    }
    return;
  }

  this.push(LONG_TYPE.encode(data.count, 10, true));
  this.push(LONG_TYPE.encode(data.buf.length));
  this.push(data.buf);
  this.push(this._syncMarker);
};

BlockEncoder.prototype._createBlockCallback = function () {
  var self = this;
  var index = this._index++;
  var count = this._blockCount;
  this._pending++;

  return function (err, data) {
    if (err) {
      self.emit('error', err);
      return;
    }
    self._pending--;
    self._queue.push(new BlockData(index, data, count));
    if (self._needPush) {
      self._needPush = false;
      self._read();
    }
  };
};



// Helpers.

/**
 * An indexed block.
 *
 * This can be used to preserve block order since compression and decompression
 * can cause some some blocks to be returned out of order. The count is only
 * used when encoding.
 *
 */
function BlockData(index, buf, count) {
  this.index = index;
  this.buf = buf;
  this.count = count | 0;
}


/**
 * Maybe get a block.
 *
 */
function tryReadBlock(tap) {
  var pos = tap.pos;
  var block = BLOCK_TYPE._read.call(tap);
  if (!tap.isValid()) {
    tap.pos = pos;
    return null;
  }
  return block;
}

function createReader(decode, type) {
  if (decode) {
    return type._read;
  } else {
    return (function (skipper) {
      return function () {
        var pos = this.pos;
        skipper.call(this);
        return this.buf.slice(pos, this.pos);
      };
    })(type._skip);
  }
}


module.exports = {
  BlockDecoder: BlockDecoder,
  RawDecoder: RawDecoder,
  BlockEncoder: BlockEncoder,
  RawEncoder: RawEncoder
};
