/* jshint node: true */

// TODO: Add frame decoder and encoder (for future use with protocols).

'use strict';


var types = require('./types'),
    Tap = require('./tap'),
    utils = require('./utils'),
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

// Used to toBuffer each block, without having to copy all its data.
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
  checkIsType(type);
  opts = opts || {};

  var decode = opts.decode === undefined ? true : !!opts.decode;
  stream.Duplex.call(this, {
    readableObjectMode: decode,
    allowHalfOpen: false
  });
  // Somehow setting this to false only closes the writable side after the
  // readable side ends, while we need the other way. So we do it manually.

  this._type = type;
  this._tap = new Tap(new Buffer(0));
  this._needPush = false;
  this._readObj = createReader(decode, this._type);
  this._finished = false;

  this.on('finish', function () {
    this._finished = true;
    this._read();
  });
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
  var obj = this._readObj(tap);
  if (tap.isValid()) {
    this.push(obj);
  } else if (!this._finished) {
    tap.pos = pos;
    this._needPush = true;
  } else {
    this.push(null);
  }
};


/**
 * Duplex stream for decoding frames.
 *
 * @param opts {Object}
 *
 *  + type
 *  + decode
 *
 */
function FrameDecoder(type, opts) {
  checkIsType(type);
  opts = opts || {};

  var decode = opts.decode === undefined ? true : !!opts.decode;
  stream.Duplex.call(this, {
    readableObjectMode: decode,
    allowHalfOpen: false
  });

  this._type = type;
  this._tap = new Tap(new Buffer(0));
  this._decode = decode;
  this._needPush = false;
  this._frameLength = -1;
  this._finished = false;

  this.on('finish', function () {
    this._finished = true;
    this._read();
  });
}
util.inherits(FrameDecoder, stream.Duplex);

FrameDecoder.prototype._write = function (chunk, encoding, cb) {
  var tap = this._tap;
  tap.buf = Buffer.concat([tap.buf.slice(tap.pos), chunk]);
  tap.pos = 0;
  if (this._needPush) {
    this._needPush = false;
    this._read();
  }
  cb();
};

FrameDecoder.prototype._read = function () {
  var tap = this._tap;
  var buf = tap.buf;
  var pos = tap.pos;
  var len = buf.length;

  if (this._frameLength < 0) {
    if (this._finished) {
      this.push(null);
      return;
    }
    if (pos + 4 > len) {
      this._needPush = true;
      return;
    }
    tap.pos += 4;
    this._frameLength = buf.readUIntBE(pos, 4);
  }

  if (!this._frameLength) {
    this.push(null); // This will close the writable side as well.
    return;
  }

  if (tap.pos + this._frameLength > len) {
    this._needPush = true;
    return;
  }

  if (this._decode) {
    this._frameLength = -1;
    this.push(this._type._read(tap));
  } else {
    pos = tap.pos;
    tap.pos += this._frameLength;
    this._frameLength = -1;
    this.push(buf.slice(pos, tap.pos));
  }
};


/**
 * Duplex stream for decoding object container files.
 *
 * @param opts {Object}
 *
 *  + parseOpts
 *  + decode
 *  + codecs
 *
 */
function BlockDecoder(opts) {
  opts = opts || {};

  var decode = opts.decode === undefined ? true : !!opts.decode;
  stream.Duplex.call(this, {
    allowHalfOpen: true, // For async decompressors.
    readableObjectMode: decode
  });

  this._type = null;
  this._codecs = opts.codecs;
  this._parseOpts = opts.parseOpts || {};
  this._tap = new Tap(new Buffer(0));
  this._blockTap = new Tap(new Buffer(0));
  this._syncMarker = null;
  this._readObj = null;
  this._decode = decode;
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

BlockDecoder.getDefaultCodecs = function () {
  return {
    'null': function (buf, cb) { cb(null, buf); },
    'deflate': zlib.inflateRaw
  };
};

BlockDecoder.prototype._decodeHeader = function () {
  var tap = this._tap;
  var header = HEADER_TYPE._read(tap);
  if (!tap.isValid()) {
    // Wait until more data arrives.
    return false;
  }

  if (!MAGIC_BYTES.equals(header.magic)) {
    this.emit('error', new AvscError('invalid magic bytes'));
    return;
  }

  var codec = (header.meta['avro.codec'] || 'null').toString();
  this._decompress = (this._codecs || BlockDecoder.getDefaultCodecs())[codec];
  if (!this._decompress) {
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

  this.push(this._readObj(tap)); // The read is guaranteed valid.
};


/**
 * Duplex stream for encoding.
 *
 * @param type
 * @param opts {Object}
 *
 *  + batchSize
 *  + noCheck
 *
 */
function RawEncoder(type, opts) {
  checkIsType(type);
  opts = opts || {};

  stream.Transform.call(this, {
    writableObjectMode: true,
    allowHalfOpen: false
  });

  this._type = type;
  this._writeObj = function (tap, obj) { this._type._write(tap, obj); };
  this._tap = new Tap(new Buffer(opts.batchSize || 65536));
}
util.inherits(RawEncoder, stream.Transform);

RawEncoder.prototype._transform = function (obj, encoding, cb) {
  var tap = this._tap;
  var buf = tap.buf;
  var pos = tap.pos;

  if (!this._noCheck && !this._type.isValid(obj)) {
    this.emit('error', new AvscError('invalid object: %j', obj));
    return;
  }

  this._writeObj(tap, obj);
  if (!tap.isValid()) {
    if (pos) {
      // Emit any valid data.
      this.push(copyBuffer(tap.buf, 0, pos));
    }
    var len = tap.pos - pos;
    if (len > buf.length) {
      // Not enough space for last written object, need to resize.
      tap.buf = new Buffer(2 * len);
    }
    tap.pos = 0;
    this._writeObj(tap, obj); // Rewrite last failed write.
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
 * Duplex streams which encodes and frames each object.
 *
 * This is useful particularly for Avro protocol messages:
 * http://avro.apache.org/docs/current/spec.html#Message+Framing
 *
 * Currently each frame corresponds to a single encoded object.
 *
 */
function FrameEncoder(type, opts) {
  checkIsType(type);
  opts = opts || {};

  stream.Transform.call(this, {
    writableObjectMode: true,
    allowHalfOpen: false
  });

  this._type = type;
  this._noCheck = opts.noCheck;
  this._frameSize = opts.frameSize || 1024;
  this._tap = new Tap(new Buffer(this._frameSize));
}
util.inherits(FrameEncoder, stream.Transform);

FrameEncoder.prototype._transform = function (obj, encoding, cb) {
  if (!this._noCheck && !this._type.isValid(obj)) {
    cb(new AvscError('invalid object: %j', obj));
    return;
  }

  var tap = this._tap;
  tap.pos = 4;
  this._type._write(tap, obj);
  if (!tap.isValid()) {
    this._frameSize = 2 * tap.pos;
    tap.buf = new Buffer(this._frameSize);
    tap.pos = 4;
    this._type._write(tap, obj);
  }
  tap.buf.writeUIntBE(tap.pos - 4, 0, 4);
  cb(null, copyBuffer(tap.buf, 0, tap.pos));
};

FrameEncoder.prototype._flush = function (cb) {
  var buf = new Buffer(4);
  buf.fill(0);
  this.push(buf);
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
 *  + `codecs`
 *  + `omitHeader`, useful to append to an existing block file.
 *  + `noCheck`
 *
 */
function BlockEncoder(type, opts) {
  checkIsType(type);
  opts = opts || {};

  stream.Duplex.call(this, {
    allowHalfOpen: true, // To support async compressors.
    writableObjectMode: true
  });

  this._type = type;
  this._writeObj = function (tap, obj) { this._type._write(tap, obj); };
  this._noCheck = opts.noCheck;
  this._blockSize = opts.blockSize || 65536;
  this._tap = new Tap(new Buffer(this._blockSize));
  this._codecs = opts.codecs;
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

BlockEncoder.getDefaultCodecs = function () {
  return {
    'null': function (buf, cb) { cb(null, buf); },
    'deflate': zlib.deflateRaw
  };
};

BlockEncoder.prototype._write = function (obj, encoding, cb) {
  var codec = this._codec;
  this._compress = (this._codecs || BlockEncoder.getDefaultCodecs())[codec];
  if (!this._compress) {
    this.emit('error', new AvscError('unsupported codec: %s', codec));
    return;
  }

  if (!this._omitHeader) {
    var meta = {
      'avro.schema': new Buffer(this._type.toString()),
      'avro.codec': new Buffer(this._codec)
    };
    var Header = HEADER_TYPE.getRecordConstructor();
    var header = new Header(MAGIC_BYTES, meta, this._syncMarker);
    this.push(header.$toBuffer());
  }

  this._write = this._writeChunk;
  this._write(obj, encoding, cb);
};

BlockEncoder.prototype._writeChunk = function (obj, encoding, cb) {
  var tap = this._tap;
  var pos = tap.pos;

  if (!this._noCheck && !this._type.isValid(obj)) {
    this.emit('error', new AvscError('invalid object: %j', obj));
    return;
  }

  this._writeObj(tap, obj);
  if (!tap.isValid()) {
    if (pos) {
      this._flushChunk(pos);
    }
    var len = tap.pos - pos;
    if (len > this._blockSize) {
      // Not enough space for last written object, need to resize.
      this._blockSize = len * 2;
    }
    tap.buf = new Buffer(this._blockSize);
    tap.pos = 0;
    this._writeObj(tap, obj); // Rewrite last failed write.
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

  this.push(LONG_TYPE.toBuffer(data.count, true));
  this.push(LONG_TYPE.toBuffer(data.buf.length, true));
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
  var block = BLOCK_TYPE._read(tap);
  if (!tap.isValid()) {
    tap.pos = pos;
    return null;
  }
  return block;
}

/**
 * Check whether an argument is a type and return a helpful error message.
 *
 */
function checkIsType(type) {
  if (!type) {
    throw new AvscError('missing type');
  }
  if (!(type instanceof types.Type)) {
    throw new AvscError('not a type: %j', type);
  }
}

/**
 * Create bytes consumer, either reading or skipping records.
 *
 */
function createReader(decode, type) {
  if (decode) {
    return function (tap) { return type._read(tap); };
  } else {
    return (function (skipper) {
      return function (tap) {
        var pos = tap.pos;
        skipper(tap);
        return tap.buf.slice(pos, tap.pos);
      };
    })(type._skip);
  }
}

/**
 * Copy a buffer.
 *
 * This avoids having to create a slice of the original buffer.
 *
 */
function copyBuffer(buf, pos, len) {
  var copy = new Buffer(len);
  buf.copy(copy, 0, pos, pos + len);
  return copy;
}


module.exports = {
  BLOCK_TYPE: BLOCK_TYPE,
  HEADER_TYPE: HEADER_TYPE,
  MAGIC_BYTES: MAGIC_BYTES,
  streams: {
    RawDecoder: RawDecoder,
    FrameDecoder: FrameDecoder,
    BlockDecoder: BlockDecoder,
    RawEncoder: RawEncoder,
    FrameEncoder: FrameEncoder,
    BlockEncoder: BlockEncoder
  }
};
