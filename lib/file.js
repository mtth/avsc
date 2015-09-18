/* jshint node: true */

// TODO: Support fragments.

'use strict';

var parse = require('./parse'),
    Tap = require('./tap'),
    stream = require('stream'),
    util = require('util');
    // zlib = require('zlib');


// First four bytes of each Avro file.
var MAGIC_BYTES = new Buffer(['0x4f', '0x62', '0x6a', '0x01']);

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

/**
 * Decode a container block file stream.
 *
 * This is a duplex stream.
 *
 */
function Decoder(opts) {

  opts = opts || {};
  var tap = new Tap(new Buffer(0));
  var blockTap = new Tap(new Buffer(0));

  this._header = null;
  this.writerType = opts.writerType;

  stream.Transform.call(this, {objectMode: true});

  this.on('pipe', function () {

    if (this.writerType) {
      this.emit('error', new Error('cannot reuse container decoder'));
    }

  });

  // Header parser transformer.
  this._transform = function (data, encoding, callback) {

    if (!tap) {
      tap = new Tap(data);
    } else {
      tap.buf = Buffer.concat([tap.buf.slice(tap.pos), data]);
      tap.pos = 0;
    }

    if (!this._header && !decodeHeader(this)) {
      callback(); // Wait for more data.
    }

    var self = this;
    var len = tap.buf.length;
    do {
      var pos = tap.pos;
      var block = BLOCK_TYPE._read.call(tap);
    } while (processBlock(block));
    callback();

    // Process block and check whether there are more bytes to read.
    function processBlock(block) {

      if (!tap.isValid()) {
        tap.pos = pos;
        return false;
      }

      if (!self._header.sync.equals(block.sync)) {
        self.emit('error', new Error('invalid sync marker'));
        // TODO: Try to recover?
      }

      var blockData = block.data;
      var blockSize = blockData.length;
      blockTap.buf = blockData;
      blockTap.pos = 0;
      while (blockTap.pos < blockSize) {
        self.push(self.writerType._read.call(blockTap));
      }

      return tap.pos < len;

    }

  };

  function decodeHeader(self) {

    var header = HEADER_TYPE._read.call(tap);
    if (!tap.isValid()) {
      // Wait until more data arrives.
      tap.pos = 0;
      return false;
    }

    if (!MAGIC_BYTES.equals(header.magic)) {
      throw new Error('invalid magic bytes');
    }

    var meta = header.meta;
    var codec = meta['avro.codec'].toString();
    switch (codec) {
      case 'null':
        // self._decompress = null; // TODO.
        break;
      case 'deflate':
        // self._decompress = zlib.deflateRaw;
        break;
      default:
        throw new Error('unknown codec: ' + codec);
    }

    var schema = JSON.parse(header.meta['avro.schema'].toString());
    self.writerType = parse.parse(schema);
    self._header = header;
    self.emit('metadata', self.writerType, codec);
    return true;

  }

}
util.inherits(Decoder, stream.Transform);

module.exports = {
  Decoder: Decoder
};
