/* jshint node: true */

'use strict';

var parse = require('./parse'),
    stream = require('stream'),
    util = require('util'),
    zlib = require('zlib');

var headerType = parse.parse({
  type: 'record',
  name: 'org.apache.avro.file.Header',
  fields : [
    {name: 'magic', type: {type: 'fixed', name: 'Magic', size: 4}},
    {name: 'meta', type: {type: 'map', values: 'bytes'}},
    {name: 'sync', type: {type: 'fixed', name: 'Sync', size: 16}}
  ]
});

/**
 * Decode a container block file stream.
 *
 * This is a readable stream. This will return block by block.
 *
 */
function BlockReader(source) {

  stream.Readable.call(this, {objectMode: true});
  this._meta = null;
  this._sync = null;

  var buf = new Buffer();

  source.on('data', function (chunk) {

  });

}

BlockReader.prototype._read = function () {

  

};

util.inherits(BlockReader, stream.Readable);
