/* jshint node: true */

'use strict';

var parse = require('./parse'),
    Tap = require('./tap'),
    stream = require('stream'),
    util = require('util'),
    zlib = require('zlib');


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
 * This is a readable stream. This will return block by block.
 *
 * @param stream {Stream} Stream of bytes from an Avro block file.
 *
 */
function Decoder(source) {

  stream.Readable.call(this);

  var self = this;
  var tap = new Tap(new Buffer(0));
  var blockTap = new Tap(new Buffer(0));

  this._header = null;
  this._decompress = null;
  this.writerType = null;
  var time;

  source.on('data', function (chunk) {

    tap.buf = Buffer.concat([tap.buf.slice(tap.offset), chunk]);
    tap.offset = 0;

    if (!self._header && !decodeHeader()) {
      return; // TODO: Allow fragments.
    }

    var len = tap.buf.length;
    do {
      var offset = tap.offset;
      var block = BLOCK_TYPE._read.call(tap);
    } while (processBlock(block));

    // Process block and check whether there are more bytes to read.
    function processBlock(block) {

      if (!tap.isValid()) {
        tap.offset = offset;
        return false;
      }

      if (!self._header.sync.equals(block.sync)) {
        throw new Error('invalid sync marker'); // TODO: Try to recover?
      }

      var buf = block.data;
      var size = buf.length;
      blockTap.buf = buf;
      blockTap.offset = 0;

      var obj;
      while (blockTap.offset < size) {
        obj = self.writerType._read.call(blockTap);
        if (obj.header === undefined) {
          throw new Error('no');
        }
        // var s = JSON.stringify(obj);
        // console.log(s);
      }

      return tap.offset < len;

    }

  });
  source.on('end', function () {
    time = process.hrtime(time);
    console.error(time[0] + time[1] * 1e-9);
  });

  function decodeHeader() {

    var header = HEADER_TYPE._read.call(tap);
    if (!tap.isValid()) {
      // Wait until more data arrives.
      tap.offset = 0;
      return false;
    }

    if (!MAGIC_BYTES.equals(header.magic)) {
      throw new Error('invalid magic bytes');
    }

    var meta = header.meta;
    var codec = meta['avro.codec'].toString();
    switch (codec) {
      case 'null':
        self._decompress = null; // TODO.
        break;
      case 'deflate':
        self._decompress = zlib.deflateRaw;
        break;
      default:
        throw new Error('unknown codec: ' + codec);
    }

    var schema = JSON.parse(header.meta['avro.schema'].toString());
    self.writerType = parse.parse(schema);

    self._header = header;
    time = process.hrtime();
    return true;

  }

}
util.inherits(Decoder, stream.Readable);

Decoder.prototype._read = function () {

};

module.exports = {
  Decoder: Decoder
};
