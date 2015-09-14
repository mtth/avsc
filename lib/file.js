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
function BlockReader(source) {

  stream.Readable.call(this);

  var self = this;
  var tap = new Tap(new Buffer(0));

  this._header = null;
  this._decompressor = null;

  source.on('data', function (chunk) {

    tap.buf = Buffer.concat([tap.buf.slice(tap.offset), chunk]);
    tap.offset = 0;

    if (!self._header) {

      var header = HEADER_TYPE.read(tap);
      if (!tap.isValid()) {
        // Wait until more data arrives.
        tap.offset = 0;
        return;
      }

      if (!MAGIC_BYTES.equals(header.magic)) {
        throw new Error('invalid magic bytes');
      }

      var meta = header.meta;
      var codec = meta['avro.codec'].toString();
      switch (codec) {
        case 'null':
          self._decompressor = null; // TODO.
          break;
        case 'deflate':
          self._decompressor = zlib.deflateRaw;
          break;
        default:
          throw new Error('unknown codec: ' + codec);
      }
      self._header = header;

    }

    var len = tap.buf.length;
    do {
      var offset = tap.offset;
      var block = BLOCK_TYPE.read(tap);
    } while (hasNext(block));

    // Process block and check whether there are more bytes to read.
    function hasNext(block) {

      if (!tap.isValid()) {
        tap.offset = offset;
        return false;
      }

      if (!self._header.sync.equals(block.sync)) {
        throw new Error('invalid sync marker');
        // TODO: Try to recover from this?
      }

      return tap.offset < len;

    }

  });

}
util.inherits(BlockReader, stream.Readable);

/**
 * Get parsed type.
 *
 * This is semi-expensive so should be cached.
 *
 */
BlockReader.prototype.getType = function () {

  if (!this._meta) {
    throw new Error('metadata not available yet');
  }
  return parse.parse(JSON.parse(this._meta['avro.schema'].toString()));

};

BlockReader.prototype._read = function () {

};

module.exports = {
  BlockReader: BlockReader
};
