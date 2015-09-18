/* jshint node: true */

// TODO: Support fragments.

'use strict';

var parse = require('./parse'),
    Tap = require('./tap'),
    stream = require('stream'),
    util = require('util'),
    zlib = require('zlib');


// Type of Avro header.
var HEADER_TYPE = parse.parse({
  type: 'record',
  name: 'org.apache.avro.file.Header',
  fields : [
    {name: 'magic', type: {type: 'fixed', name: 'Magic', size: 4}},
    {name: 'meta', type: {type: 'map', values: 'bytes'}},
    {name: 'sync', type: {type: 'fixed', name: 'Sync', size: 16}}
  ]
});

// Type of each block.
var BLOCK_TYPE = parse.parse({
  type: 'record',
  name: 'org.apache.avro.file.Block',
  fields : [
    {name: 'count', type: 'long'},
    {name: 'data', type: 'bytes'},
    {name: 'sync', type: {type: 'fixed', name: 'Sync', size: 16}}
  ]
});

// First four bytes of each Avro file.
var MAGIC_BYTES = new Buffer('Obj\x01');
var MAGIC_TYPE = HEADER_TYPE.fields[0].type;

/**
 * Decode a container block file stream.
 *
 * This is a duplex stream.
 *
 */
function BlockDecoder(opts) {

  opts = opts || {};

  stream.Transform.call(this, {readableObjectMode: true});

  this._tap = new Tap(new Buffer(0));
  this._writerType = null;
  this._sync = null;

  this.on('pipe', function () {

    if (this._writerType) {
      this.emit('error', new Error('cannot reuse block decoder'));
    }

  });

  this._transform = function (chunk, encoding, cb) {

    var tap = this._tap;
    tap.buf = Buffer.concat([tap.buf, chunk]);

    var header = HEADER_TYPE._read.call(tap);
    if (!tap.isValid()) {
      // Wait until more data arrives.
      tap.pos = 0;
      cb();
      return;
    }

    if (!MAGIC_BYTES.equals(header.magic)) {
      cb(new Error('invalid magic bytes'));
      return;
    }

    var codec = header.meta['avro.codec'].toString();
    var schema = JSON.parse(header.meta['avro.schema'].toString());
    try { // TODO: Put try catch in helper function?
      var type = parse.parse(schema);
    } catch (err) {
      cb(err);
      return;
    }

    this._writerType = type;
    this._sync = header.sync;
    this.emit('metadata', codec, this._sync);

    this._transform = function (chunk, encoding, cb) {

      var tap = this._tap;
      tap.buf = Buffer.concat([tap.buf.slice(tap.pos), chunk]);
      tap.pos = 0;

      var block;
      while ((block = getBlock())) {
        if (!this._sync.equals(block.sync)) {
          cb(new Error('invalid sync marker'));
          return; // TODO: Try to recover?
        }
        this.push(block);
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

    this._transform(new Buffer(0), encoding, cb);

  };

}
util.inherits(BlockDecoder, stream.Transform);

BlockDecoder.prototype.getWriterType = function () {

  return this._writerType;

};


function RecordDecoder(opts) {

  opts = opts || {};

  stream.Transform.call(this, {objectMode: true});

  this._tap = new Tap(null);
  this._readerType = opts.type;
  this._writerType = null;
  this._sync = null;
  this._decompress = null;

  this.on('pipe', function (src) {

    var self = this;

    if (this._writerType) {
      this.emit('error', new Error('cannot reuse block decoder'));
      return;
    }

    src.once('metadata', function (codec, sync) {

      switch (codec) {
        case 'null':
          break;
        case 'deflate':
          self._decompress = zlib.deflateRawSync;
          break;
        default:
          self.emit(new Error('unknown codec: ' + codec));
          return;
      }

      self._writerType = src.getWriterType();
      self._sync = sync;
      self._readerType = null; // TODO

    });

  });

  this._transform = function (block, encoding, cb) {

    var tap = this._tap;
    tap.buf = this._decompress ? this._decompress(block.data) : block.data;
    tap.pos = 0;

    var recordCount = block.count;
    while (recordCount--) {
      this.push(this._writerType._read.call(tap));
    }
    cb();

  };

}
util.inherits(RecordDecoder, stream.Transform);

/**
 * Decode a container block file stream.
 *
 * This is a duplex stream.
 *
 */
function Decoder(opts) {

  opts = opts || {};

  stream.Transform.call(this, {readableObjectMode: true});

  var self = this;

  this._isContainerFile = true;
  this._tap = new Tap(new Buffer(0));
  this._readerType = null;
  this._writerType = null;

  this.on('pipe', function () {

    if (this._readerType) {
      this.emit('error', new Error('cannot reuse decoder'));
    }

  });

  this._transform = function (chunk, encoding, cb) {

    var tap = this._tap;
    tap.buf = Buffer.concat([tap.buf, chunk]);
    if (tap.buf.length < 4) {
      cb();
      return;
    }

    var bytes = MAGIC_TYPE._read.call(tap);
    tap.pos = 0;
    if (opts.containerFile === false || !MAGIC_BYTES.equals(bytes)) {
      // Decoding fragments.
      if (!opts.type) {
        cb(new Error('need type to decode fragments'));
        return;
      }
      this._isContainerFile = false;
      this._readerType = opts.type;
      this._transform = fragmentsTransformer;
    } else {
      this._transform = headerTransformer;
    }
    this._transform(new Buffer(0), encoding, cb);

  };

  function fragmentsTransformer(chunk, encoding, cb) {

    // TODO.
    cb(new Error('not implemented'));

  }

  function headerTransformer(chunk, encoding, cb) {

    var tap = self._tap;
    tap.buf = Buffer.concat([tap.buf.slice(tap.pos), chunk]);
    tap.pos = 0;

    var header = HEADER_TYPE._read.call(tap);
    if (!tap.isValid()) {
      // Wait until more data arrives.
      tap.pos = 0;
      cb();
      return;
    }

    if (!MAGIC_BYTES.equals(header.magic)) {
      cb(new Error('invalid magic bytes')); // This should never happen.
      return;
    }

    var codec = header.meta['avro.codec'].toString();
    var decompressor;
    switch (codec) {
      case 'null':
        decompressor = null;
        break;
      case 'deflate':
        decompressor = zlib.deflateRawSync;
        break;
      default:
        cb(new Error('unknown codec: ' + codec));
        return;
    }

    var schema = JSON.parse(header.meta['avro.schema'].toString());
    try { // TODO: Put try catch in helper function?
      var type = parse.parse(schema);
    } catch (err) {
      cb(err);
      return;
    }

    self._writerType = type;
    self._readerType = opts.type ? opts.type.asReaderOf(type) : type;
    self.emit('metadata', codec, header.sync);

    self._transform = getBlocksTransformer(header.sync, decompressor);
    self._transform(new Buffer(0), encoding, cb);

  }

  function getBlocksTransformer(sync, decompressor) {

    var blockTap = new Tap(null);

    return function (chunk, encoding, cb) {

      console.log('transforming');
      var tap = self._tap;
      tap.buf = Buffer.concat([tap.buf.slice(tap.pos), chunk]);
      tap.pos = 0;

      var block, pos;
      do {
        pos = tap.pos;
        block = BLOCK_TYPE._read.call(tap);
      } while (tap.isValid() && decodeBlock(block, cb));

      tap.pos = pos;
      cb();

    };

    function decodeBlock(block, cb) {

      var blockData = decompressor ? decompressor(block.data) : block.data;
      // var len = tap.buf.length;
      if (!sync.equals(block.sync)) {
        cb(new Error('invalid sync marker'));
        return; // TODO: Try to recover?
      }
      var blockSize = blockData.length;
      blockTap.buf = blockData;
      blockTap.pos = 0;
      while (blockTap.pos < blockSize) {
        self.push(self._writerType._read.call(blockTap));
      }

    }

  }

}
util.inherits(Decoder, stream.Transform);

Decoder.prototype.getReaderType = function () {

  if (!this._readerType) {
    throw new Error('reader type not yet available');
  }
  return this._readerType;

};

Decoder.prototype.getWriterType = function () {

  if (!this._isContainerFile) {
    throw new Error('no writer type available when reading fragments');
  }
  if (this._writerType === null) {
    throw new Error('writer type not yet available');
  }
  return this._writerType;

};

module.exports = {
  BlockDecoder: BlockDecoder,
  RecordDecoder: RecordDecoder,
  Decoder: Decoder
};
