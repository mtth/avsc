// TODO: Add streams which prefix each record with its length.

'use strict';

/**
 * This module defines custom streams to write and read Avro files.
 *
 * In particular, the `Block{En,De}coder` streams are able to deal with Avro
 * container files. None of the streams below depend on the filesystem however,
 * this way they can also be used in the browser (for example to parse HTTP
 * responses).
 */

let types = require('./types'),
    utils = require('./utils'),
    buffer = require('buffer'),
    stream = require('stream'),
    zlib = require('zlib');

let Buffer = buffer.Buffer;
let OPTS = {namespace: 'org.apache.avro.file'};

let LONG_TYPE = types.Type.forSchema('long', OPTS);

let MAP_BYTES_TYPE = types.Type.forSchema({type: 'map', values: 'bytes'}, OPTS);

let HEADER_TYPE = types.Type.forSchema({
  name: 'Header',
  type: 'record',
  fields : [
    {name: 'magic', type: {type: 'fixed', name: 'Magic', size: 4}},
    {name: 'meta', type: MAP_BYTES_TYPE},
    {name: 'sync', type: {type: 'fixed', name: 'Sync', size: 16}}
  ]
}, OPTS);

let BLOCK_TYPE = types.Type.forSchema({
  name: 'Block',
  type: 'record',
  fields : [
    {name: 'count', type: 'long'},
    {name: 'data', type: 'bytes'},
    {name: 'sync', type: 'Sync'}
  ]
}, OPTS);

// First 4 bytes of an Avro object container file.
let MAGIC_BYTES = utils.bufferFrom('Obj\x01');

// Convenience.
let Tap = utils.Tap;


/** Duplex stream for decoding fragments. */
class RawDecoder extends stream.Duplex {
  constructor (schema, opts) {
    opts = opts || {};

    let noDecode = !!opts.noDecode;
    super({
      readableObjectMode: !noDecode,
      allowHalfOpen: false
    });

    this._type = types.Type.forSchema(schema);
    this._tap = new Tap(utils.newBuffer(0));
    this._writeCb = null;
    this._needPush = false;
    this._readValue = createReader(noDecode, this._type);
    this._finished = false;

    this.on('finish', function () {
      this._finished = true;
      this._read();
    });
  }

  _write (chunk, encoding, cb) {
    // Store the write callback and call it when we are done decoding all
    // records in this chunk. If we call it right away, we risk loading the
    // entire input in memory. We only need to store the latest callback since
    // the stream API guarantees that `_write` won't be called again until we
    // call the previous.
    this._writeCb = cb;

    let tap = this._tap;
    tap.buf = Buffer.concat([tap.buf.slice(tap.pos), chunk]);
    tap.pos = 0;
    if (this._needPush) {
      this._needPush = false;
      this._read();
    }
  }

  _read () {
    this._needPush = false;

    let tap = this._tap;
    let pos = tap.pos;
    let val = this._readValue(tap);
    if (tap.isValid()) {
      this.push(val);
    } else if (!this._finished) {
      tap.pos = pos;
      this._needPush = true;
      if (this._writeCb) {
        // This should only ever be false on the first read, and only if it
        // happens before the first write.
        this._writeCb();
      }
    } else {
      this.push(null);
    }
  }
}


/** Duplex stream for decoding object container files. */
class BlockDecoder extends stream.Duplex {
  constructor (opts) {
    opts = opts || {};

    let noDecode = !!opts.noDecode;
    super({
      allowHalfOpen: true, // For async decompressors.
      readableObjectMode: !noDecode
    });

    this._rType = opts.readerSchema !== undefined ?
      types.Type.forSchema(opts.readerSchema) :
      undefined;
    this._wType = null;
    this._codecs = opts.codecs;
    this._codec = undefined;
    this._parseHook = opts.parseHook;
    this._tap = new Tap(utils.newBuffer(0));
    this._blockTap = new Tap(utils.newBuffer(0));
    this._syncMarker = null;
    this._readValue = null;
    this._noDecode = noDecode;
    this._queue = new utils.OrderedQueue();
    this._decompress = null; // Decompression function.
    this._index = 0; // Next block index.
    this._remaining = undefined; // In the current block.
    this._needPush = false;
    this._finished = false;

    this.on('finish', function () {
      this._finished = true;
      if (this._needPush) {
        this._read();
      }
    });
  }

  static defaultCodecs () {
    return {
      'null': function (buf, cb) { cb(null, buf); },
      'deflate': zlib.inflateRaw
    };
  }

  static getDefaultCodecs () {
    return BlockDecoder.defaultCodecs();
  }

  _decodeHeader () {
    let tap = this._tap;
    if (tap.buf.length < MAGIC_BYTES.length) {
      // Wait until more data arrives.
      return false;
    }

    if (!MAGIC_BYTES.equals(tap.buf.slice(0, MAGIC_BYTES.length))) {
      this.emit('error', new Error('invalid magic bytes'));
      return false;
    }

    let header = HEADER_TYPE._read(tap);
    if (!tap.isValid()) {
      return false;
    }

    this._codec = (header.meta['avro.codec'] || 'null').toString();
    let codecs = this._codecs || BlockDecoder.getDefaultCodecs();
    this._decompress = codecs[this._codec];
    if (!this._decompress) {
      this.emit('error', new Error(`unknown codec: ${this._codec}`));
      return;
    }

    try {
      let schema = JSON.parse(header.meta['avro.schema'].toString());
      if (this._parseHook) {
        schema = this._parseHook(schema);
      }
      this._wType = types.Type.forSchema(schema);
    } catch (err) {
      this.emit('error', err);
      return;
    }

    try {
      this._readValue = createReader(this._noDecode, this._wType, this._rType);
    } catch (err) {
      this.emit('error', err);
      return;
    }

    this._syncMarker = header.sync;
    this.emit('metadata', this._wType, this._codec, header);
    return true;
  }

  _write (chunk, encoding, cb) {
    let tap = this._tap;
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
    this._write(utils.newBuffer(0), encoding, cb);
  }

  _writeChunk (chunk, encoding, cb) {
    let tap = this._tap;
    tap.buf = Buffer.concat([tap.buf.slice(tap.pos), chunk]);
    tap.pos = 0;

    let nBlocks = 1;
    let block;
    while ((block = tryReadBlock(tap))) {
      if (!this._syncMarker.equals(block.sync)) {
        this.emit('error', new Error('invalid sync marker'));
        return;
      }
      nBlocks++;
      this._decompress(
        block.data,
        this._createBlockCallback(block.data.length, block.count, chunkCb)
      );
    }
    chunkCb();

    function chunkCb() {
      if (!--nBlocks) {
        cb();
      }
    }
  }

  _createBlockCallback (size, count, cb) {
    let self = this;
    let index = this._index++;

    return function (cause, data) {
      if (cause) {
        let err = new Error(`${self._codec} codec decompression error`);
        err.cause = cause;
        self.emit('error', err);
        cb();
      } else {
        self.emit('block', new BlockInfo(count, data.length, size));
        self._queue.push(new BlockData(index, data, cb, count));
        if (self._needPush) {
          self._read();
        }
      }
    };
  }

  _read () {
    this._needPush = false;

    let tap = this._blockTap;
    if (!this._remaining) {
      let data = this._queue.pop();
      if (!data || !data.count) {
        if (this._finished) {
          this.push(null);
        } else {
          this._needPush = true;
        }
        if (data) {
          data.cb();
        }
        return; // Wait for more data.
      }
      data.cb();
      this._remaining = data.count;
      tap.buf = data.buf;
      tap.pos = 0;
    }

    this._remaining--;
    let val;
    try {
      val = this._readValue(tap);
      if (!tap.isValid()) {
        throw new Error('truncated block');
      }
    } catch (err) {
      this._remaining = 0;
      this.emit('error', err); // Corrupt data.
      return;
    }
    this.push(val);
  }
}


/** Duplex stream for encoding. */
class RawEncoder extends stream.Transform {
  constructor (schema, opts) {
    opts = opts || {};

    super({
      writableObjectMode: true,
      allowHalfOpen: false
    });

    this._type = types.Type.forSchema(schema);
    this._writeValue = function (tap, val) {
      try {
        this._type._write(tap, val);
      } catch (err) {
        this.emit('typeError', err, val, this._type);
      }
    };
    this._tap = new Tap(utils.newBuffer(opts.batchSize || 65536));

    this.on('typeError', function (err) { this.emit('error', err); });
  }

  _transform (val, encoding, cb) {
    let tap = this._tap;
    let buf = tap.buf;
    let pos = tap.pos;

    this._writeValue(tap, val);
    if (!tap.isValid()) {
      if (pos) {
        // Emit any valid data.
        this.push(copyBuffer(tap.buf, 0, pos));
      }
      let len = tap.pos - pos;
      if (len > buf.length) {
        // Not enough space for last written object, need to resize.
        tap.buf = utils.newBuffer(2 * len);
      }
      tap.pos = 0;
      this._writeValue(tap, val); // Rewrite last failed write.
    }

    cb();
  }

  _flush (cb) {
    let tap = this._tap;
    let pos = tap.pos;
    if (pos) {
      // This should only ever be false if nothing is written to the stream.
      this.push(tap.buf.slice(0, pos));
    }
    cb();
  }
}


/**
 * Duplex stream to write object container files.
 *
 * @param schema
 * @param opts {Object}
 *
 *  + `blockSize`, uncompressed.
 *  + `codec`
 *  + `codecs`
 *  + `metadata``
 *  + `noCheck`
 *  + `omitHeader`, useful to append to an existing block file.
 */
class BlockEncoder extends stream.Duplex {
  constructor (schema, opts) {
    opts = opts || {};

    super({
      allowHalfOpen: true, // To support async compressors.
      writableObjectMode: true
    });

    let type;
    if (types.Type.isType(schema)) {
      type = schema;
      schema = undefined;
    } else {
      // Keep full schema to be able to write it to the header later.
      type = types.Type.forSchema(schema);
    }

    this._schema = schema;
    this._type = type;
    this._writeValue = function (tap, val) {
      try {
        this._type._write(tap, val);
      } catch (err) {
        this.emit('typeError', err, val, this._type);
        return false;
      }
      return true;
    };
    this._blockSize = opts.blockSize || 65536;
    this._tap = new Tap(utils.newBuffer(this._blockSize));
    this._codecs = opts.codecs;
    this._codec = opts.codec || 'null';
    this._blockCount = 0;
    this._syncMarker = opts.syncMarker || new utils.Lcg().nextBuffer(16);
    this._queue = new utils.OrderedQueue();
    this._pending = 0;
    this._finished = false;
    this._needHeader = false;
    this._needPush = false;

    this._metadata = opts.metadata || {};
    if (!MAP_BYTES_TYPE.isValid(this._metadata)) {
      throw new Error('invalid metadata');
    }

    let codec = this._codec;
    this._compress = (this._codecs || BlockEncoder.getDefaultCodecs())[codec];
    if (!this._compress) {
      throw new Error(`unsupported codec: ${codec}`);
    }

    if (opts.omitHeader !== undefined) { // Legacy option.
      opts.writeHeader = opts.omitHeader ? 'never' : 'auto';
    }
    switch (opts.writeHeader) {
      case false:
      case 'never':
        break;
      // Backwards-compatibility (eager default would be better).
      case undefined:
      case 'auto':
        this._needHeader = true;
        break;
      default:
        this._writeHeader();
    }

    this.on('finish', function () {
      this._finished = true;
      if (this._blockCount) {
        this._flushChunk();
      } else if (this._finished && this._needPush) {
        // We don't need to check `_isPending` since `_blockCount` is always
        // positive after the first flush.
        this.push(null);
      }
    });

    this.on('typeError', function (err) { this.emit('error', err); });
  }

  static defaultCodecs () {
    return {
      'null': function (buf, cb) { cb(null, buf); },
      'deflate': zlib.deflateRaw
    };
  }

  static getDefaultCodecs () {
    return BlockEncoder.defaultCodecs();
  }

  _writeHeader () {
    let schema = JSON.stringify(
      this._schema ? this._schema : this._type.getSchema({exportAttrs: true})
    );
    let meta = utils.copyOwnProperties(
      this._metadata,
      {
        'avro.schema': utils.bufferFrom(schema),
        'avro.codec': utils.bufferFrom(this._codec)
      },
      true // Overwrite.
    );
    let Header = HEADER_TYPE.getRecordConstructor();
    let header = new Header(MAGIC_BYTES, meta, this._syncMarker);
    this.push(header.toBuffer());
  }

  _write (val, encoding, cb) {
    if (this._needHeader) {
      this._writeHeader();
      this._needHeader = false;
    }

    let tap = this._tap;
    let pos = tap.pos;
    let flushing = false;

    if (this._writeValue(tap, val)) {
      if (!tap.isValid()) {
        if (pos) {
          this._flushChunk(pos, cb);
          flushing = true;
        }
        let len = tap.pos - pos;
        if (len > this._blockSize) {
          // Not enough space for last written object, need to resize.
          this._blockSize = len * 2;
        }
        tap.buf = utils.newBuffer(this._blockSize);
        tap.pos = 0;
        this._writeValue(tap, val); // Rewrite last failed write.
      }
      this._blockCount++;
    } else {
      tap.pos = pos;
    }

    if (!flushing) {
      cb();
    }
  }

  _flushChunk (pos, cb) {
    let tap = this._tap;
    pos = pos || tap.pos;
    this._compress(tap.buf.slice(0, pos), this._createBlockCallback(pos, cb));
    this._blockCount = 0;
  }

  _read () {
    let self = this;
    let data = this._queue.pop();
    if (!data) {
      if (this._finished && !this._pending) {
        process.nextTick(() => { self.push(null); });
      } else {
        this._needPush = true;
      }
      return;
    }

    this.push(LONG_TYPE.toBuffer(data.count, true));
    this.push(LONG_TYPE.toBuffer(data.buf.length, true));
    this.push(data.buf);
    this.push(this._syncMarker);

    if (!this._finished) {
      data.cb();
    }
  }

  _createBlockCallback (size, cb) {
    let self = this;
    let index = this._index++;
    let count = this._blockCount;
    this._pending++;

    return function (cause, data) {
      if (cause) {
        let err = new Error(`${self._codec} codec compression error`);
        err.cause = cause;
        self.emit('error', err);
        return;
      }
      self._pending--;
      self.emit('block', new BlockInfo(count, size, data.length));
      self._queue.push(new BlockData(index, data, cb, count));
      if (self._needPush) {
        self._needPush = false;
        self._read();
      }
    };
  }
}


// Helpers.

/** Summary information about a block. */
class BlockInfo {
  constructor (count, raw, compressed) {
    this.valueCount = count;
    this.rawDataLength = raw;
    this.compressedDataLength = compressed;
  }
}

/**
 * An indexed block.
 *
 * This can be used to preserve block order since compression and decompression
 * can cause some some blocks to be returned out of order.
 */
class BlockData {
  constructor (index, buf, cb, count) {
    this.index = index;
    this.buf = buf;
    this.cb = cb;
    this.count = count | 0;
  }
}

/** Maybe get a block. */
function tryReadBlock(tap) {
  let pos = tap.pos;
  let block = BLOCK_TYPE._read(tap);
  if (!tap.isValid()) {
    tap.pos = pos;
    return null;
  }
  return block;
}

/** Create bytes consumer, either reading or skipping records. */
function createReader(noDecode, writerType, readerType) {
  if (noDecode) {
    return (function (skipper) {
      return function (tap) {
        let pos = tap.pos;
        skipper(tap);
        return tap.buf.slice(pos, tap.pos);
      };
    })(writerType._skip);
  } else if (readerType) {
    let resolver = readerType.createResolver(writerType);
    return function (tap) { return resolver._read(tap); };
  } else {
    return function (tap) { return writerType._read(tap); };
  }
}

/** Copy a buffer. This avoids creating a slice of the original buffer. */
function copyBuffer(buf, pos, len) {
  let copy = utils.newBuffer(len);
  buf.copy(copy, 0, pos, pos + len);
  return copy;
}


module.exports = {
  BLOCK_TYPE: BLOCK_TYPE, // For tests.
  HEADER_TYPE: HEADER_TYPE, // Idem.
  MAGIC_BYTES: MAGIC_BYTES, // Idem.
  streams: {
    BlockDecoder: BlockDecoder,
    BlockEncoder: BlockEncoder,
    RawDecoder: RawDecoder,
    RawEncoder: RawEncoder
  }
};
