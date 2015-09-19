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
// var MAGIC_TYPE = HEADER_TYPE.fields[0].type;


function ContainerDecoder() {

  stream.Transform.call(this, {readableObjectMode: true});

  this._tap = new Tap(new Buffer(0));
  this._sync = null;

  this.on('pipe', function () {

    if (this._writerType) {
      this.emit('error', new Error('cannot reuse block decoder'));
    }

  });

}
util.inherits(ContainerDecoder, stream.Transform);

ContainerDecoder.prototype._transform = function (chunk, encoding, cb) {

  var tap = this._tap;

  if (!this._sync) {

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

    this._sync = header.sync;
    this.emit('metadata', schema, codec, this._sync);

  } else {

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


function BlockDecoder(type) {

  stream.Transform.call(this, {objectMode: true});

  this._tap = new Tap(null);
  this._readerType = type;
  this._writerType = null;
  this._decompress = null;

  this.on('pipe', function (src) {

    var self = this;

    if (this._writerType) {
      this.emit('error', new Error('cannot reuse block decoder'));
      return;
    }

    src.once('metadata', function (schema, codec) {

      try {
        var type = parse.parse(schema);
      } catch (err) {
        self.emit(err);
      }

      switch (codec) {
        case 'null':
          break;
        case 'deflate':
          self._decompress = zlib.inflateRawSync;
          break;
        case 'snappy':
          self.emit(new Error('snappy not yet supported'));
          return;
        default:
          self.emit(new Error('unknown codec: ' + codec));
          return;
      }

      self._writerType = type;
      self._readerType = type; // TODO

    });

  });

}
util.inherits(BlockDecoder, stream.Transform);

BlockDecoder.prototype._transform = function (block, encoding, cb) {

  var tap = this._tap;
  if (this._decompress) {
    tap.buf = this._decompress(block.data);
  } else {
    tap.buf = block.data;
  }
  tap.pos = 0;

  var recordCount = block.count;
  while (recordCount--) {
    this.push(this._readerType._read.call(tap));
  }
  cb();

};


function FragmentDecoder(type) {

  stream.Transform.call(this, {readableObjectMode: true});

  if (!type) {
    this.emit('error', new Error('missing type'));
  }

  this._tap = new Tap(null); // Null buffer used as flag.
  this._readerType = type;

  this.on('pipe', function () {

    if (this._tap.buf) {
      this.emit('error', new Error('cannot reuse decoder'));
      return;
    }
    this._tap.buf = new Buffer(0);

  });

}
util.inherits(FragmentDecoder, stream.Transform);

FragmentDecoder.prototype._transform = function (chunk, encoding, cb) {

  var tap = this._tap;
  tap.buf = Buffer.concat([tap.buf.slice(tap.pos), chunk]);
  tap.pos = 0;

  do {
    var record = this._readerType._read.call(tap);
  } while (tap.isValid() && this.push(record));

  cb();

};


function getDecoderStream(opts) {

  opts = opts || {};
  var type = opts.type;

  if (opts.containerFile === false) {
    return new FragmentDecoder(type);
  } else {
    return new ContainerDecoder(); // .pipe(new BlockDecoder(type));
  }

}

module.exports = {
  getDecoderStream: getDecoderStream,
  streams: {
    BlockDecoder: BlockDecoder,
    ContainerDecoder: ContainerDecoder
  }
};
