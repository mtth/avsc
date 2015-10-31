/* jshint node: true */

// TODO: Add `readerType` option for `RawDecoder` and `BlockDecoder`.

'use strict';


var schemas = require('./schemas'),
    utils = require('./utils'),
    fs = require('fs'),
    stream = require('stream'),
    util = require('util'),
    zlib = require('zlib');


// Type of Avro header.
var HEADER_TYPE = schemas.createType({
  type: 'record',
  name: 'org.apache.avro.file.Header',
  fields : [
    {name: 'magic', type: {type: 'fixed', name: 'Magic', size: 4}},
    {name: 'meta', type: {type: 'map', values: 'bytes'}},
    {name: 'sync', type: {type: 'fixed', name: 'Sync', size: 16}}
  ]
});

// Type of each block.
var BLOCK_TYPE = schemas.createType({
  type: 'record',
  name: 'org.apache.avro.file.Block',
  fields : [
    {name: 'count', type: 'long'},
    {name: 'data', type: 'bytes'},
    {name: 'sync', type: {type: 'fixed', name: 'Sync', size: 16}}
  ]
});

// Used to toBuffer each block, without having to copy all its data.
var LONG_TYPE = schemas.createType('long');

// First 4 bytes of an Avro object container file.
var MAGIC_BYTES = new Buffer('Obj\x01');

// Convenience.
var f = util.format;
var Tap = utils.Tap;


/**
 * Duplex stream for decoding fragments.
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
 * Duplex stream for decoding object container files.
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
  this._typeOpts = opts.typeOpts || {};
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
    this.emit('error', new Error('invalid magic bytes'));
    return;
  }

  var codec = (header.meta['avro.codec'] || 'null').toString();
  this._decompress = (this._codecs || BlockDecoder.getDefaultCodecs())[codec];
  if (!this._decompress) {
    this.emit('error', new Error(f('unknown codec: %s', codec)));
    return;
  }

  try {
    var schema = JSON.parse(header.meta['avro.schema'].toString());
    this._type = schemas.createType(schema, this._typeOpts);
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
      cb(new Error('invalid sync marker'));
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
 */
function RawEncoder(type, opts) {
  checkIsType(type);
  opts = opts || {};

  stream.Transform.call(this, {
    writableObjectMode: true,
    allowHalfOpen: false
  });

  this._type = type;
  this._writeObj = function (tap, obj) {
    try {
      this._type._write(tap, obj);
    } catch (err) {
      this.emit('error', err);
    }
  };
  this._tap = new Tap(new Buffer(opts.batchSize || 65536));
}
util.inherits(RawEncoder, stream.Transform);

RawEncoder.prototype._transform = function (obj, encoding, cb) {
  var tap = this._tap;
  var buf = tap.buf;
  var pos = tap.pos;

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
 * Duplex stream to write object container files.
 *
 * @param type
 * @param opts {Object}
 *
 *  + `blockSize`, uncompressed.
 *  + `codec`
 *  + `codecs`
 *  + `noCheck`
 *  + `omitHeader`, useful to append to an existing block file.
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
  this._writeObj = function (tap, obj) {
    try {
      this._type._write(tap, obj);
    } catch (err) {
      this.emit('error', err);
    }
  };
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
    this.emit('error', new Error(f('unsupported codec: %s', codec)));
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
  if (MAGIC_BYTES.equals(buf.slice(0, 4))) {
    do {
      header = HEADER_TYPE._read(tap);
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
  return fs.createReadStream(path).pipe(new BlockDecoder(opts));
}


/**
 * Writable stream of records to a local Avro file.
 *
 */
function createFileEncoder(path, type, opts) {
  var encoder = new BlockEncoder(type, opts);
  encoder.pipe(fs.createWriteStream(path, {defaultEncoding: 'binary'}));
  return encoder;
}


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
    throw new Error('missing type');
  }
  if (!(type instanceof schemas.types.Type)) {
    throw new Error(f('not a type: %j', type));
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
  HEADER_TYPE: HEADER_TYPE, // For tests.
  MAGIC_BYTES: MAGIC_BYTES, // Idem.
  createFileDecoder: createFileDecoder,
  createFileEncoder: createFileEncoder,
  extractFileHeader: extractFileHeader,
  streams: {
    RawDecoder: RawDecoder,
    BlockDecoder: BlockDecoder,
    RawEncoder: RawEncoder,
    BlockEncoder: BlockEncoder
  }
};
