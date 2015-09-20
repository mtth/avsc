/* jshint node: true */

'use strict';

var parse = require('./parse'),
    Tap = require('./tap'),
    utils = require('./utils'),
    stream = require('stream'),
    util = require('util'),
    zlib = require('zlib');


var AvscError = parse.AvscError;

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

function getBlock(tap) {
  var pos = tap.pos;
  var block = BLOCK_TYPE._read.call(tap);
  if (!tap.isValid()) {
    tap.pos = pos;
    return null;
  }
  return block;
}

function decompressCb(self, index) {
  return function (err, data) {
    self._pending--;
    if (err) {
      self.emit('error', err);
      return;
    }
    // TODO: Reorder blocks here.
    self._queue.add(index, data);
    if (self._needPush) {
      self._needPush = false;
      self._read();
    }
  };
}


function Decoder(opts) {

  opts = opts || {};

  stream.Duplex.call(this, {
    allowHalfOpen: true,
    readableObjectMode: true
  });

  var self = this;
  this._tap = new Tap(new Buffer(0));
  this._blockTap = new Tap(null);
  this._readerType = opts.type;
  this._decompress = null;
  this._sync = null;
  this._writerType = null;
  this._needPush = false;
  this._queue = new utils.ConsecutiveQueue();
  this._pending = 0;
  this._finished = false;

  this.on('pipe', function (src) {
    src.on('end', function () { self._finished = true; });
  });

}
util.inherits(Decoder, stream.Duplex);

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
      this._decompress = function (buf, cb) { process.nextTick(cb, buf); };
      break;
    case 'deflate':
      this._decompress = zlib.inflateRaw;
      break;
    case 'snappy':
      this.emit(new Error('snappy not yet supported'));
      return;
    default:
      this.emit(new Error('unknown codec: ' + codec));
      return;
  }

  try {
    var schema = JSON.parse(header.meta['avro.schema'].toString());
    this._writerType = parse.parse(schema, this._parseOpts);
  } catch (err) {
    this.emit('error', err);
    return;
  }

  // TODO: Reader type.
  this._readerType = this._writerType;
  this._sync = header.sync;
  this.emit('metadata', schema, codec, this._sync);
  return true;

};

Decoder.prototype._write = function (chunk, encoding, cb) {

  var tap = this._tap;
  tap.pos = 0;
  tap.buf = Buffer.concat([tap.buf, chunk]);

  if (this._decodeHeader()) {
    this._write = this._writeBlockChunk;
  }

  cb();

};

Decoder.prototype._writeBlockChunk = function (chunk, encoding, cb) {

  var tap = this._tap;
  tap.buf = Buffer.concat([tap.buf.slice(tap.pos), chunk]);
  tap.pos = 0;
  var block;
  while ((block = getBlock(tap))) {
    if (!this._sync.equals(block.sync)) {
      cb(new AvscError('invalid sync marker'));
      return; // TODO: Try to recover?
    }
    this._pending++;
    this._decompress(block.data, decompressCb(this));
  }
  cb();

};

Decoder.prototype._read = function () {

  var self = this;
  var block = this._queue.next();
  if (!block) {
    if (this._finished && !this._pending) {
      process.nextTick(function () { self.push(null); });
    } else {
      this._needPush = true;
    }
    return;
  }

  var tap = this._blockTap;
  tap.buf = block;
  tap.pos = 0;
  var len = tap.buf.length;
  while (tap.pos < len) {
    this.push(this._writerType._read.call(tap));
  }

};


module.exports = {
  Decoder: Decoder
};
