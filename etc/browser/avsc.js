/* jshint browser: true, node: true */

'use strict';

/**
 * Main browserify entry point.
 *
 */

var containers = require('../../lib/containers'),
    files = require('./lib/files'),
    protocols = require('../../lib/protocols'),
    schemas = require('../../lib/schemas'),
    types = require('../../lib/types'),
    values = require('../../lib/values'),
    stream = require('stream'),
    util = require('util');


function parse(schema, opts) {
  var attrs = files.load(schema);
  return attrs.protocol ?
    protocols.createProtocol(attrs, opts) :
    types.createType(attrs, opts);
}

// We also add a couple browser-specific utilities to read and write blobs.

/**
 * Transform stream which lazily reads a blob's contents.
 *
 */
function BlobReader(blob, opts) {
  stream.Readable.call(this);
  opts = opts || {};

  this._batchSize = opts.batchSize || 65536;
  this._blob = blob;
  this._pos = 0;
}
util.inherits(BlobReader, stream.Readable);

BlobReader.prototype._read = function () {
  var pos = this._pos;
  if (pos >= this._blob.size) {
    this.push(null);
    return;
  }

  this._pos += this._batchSize;
  var blob = this._blob.slice(pos, this._pos, this._blob.type);
  var reader = new FileReader();
  var self = this;
  reader.addEventListener('loadend', function cb(evt) {
    reader.removeEventListener('loadend', cb, false);
    if (evt.error) {
      self.emit('error', evt.error);
    } else {
      self.push(new Buffer(reader.result));
    }
  }, false);
  reader.readAsArrayBuffer(blob);
};

/**
 * Read an Avro-container stored as a blob.
 *
 */
function createBlobDecoder(blob, opts) {
  return new BlobReader(blob).pipe(new containers.streams.BlockDecoder(opts));
}

/**
 * Transform stream which builds a blob from all data written to it.
 *
 */
function BlobWriter() {
  stream.Transform.call(this, {readableObjectMode: true});
  this._bufs = [];
}
util.inherits(BlobWriter, stream.Transform);

BlobWriter.prototype._transform = function (buf, encoding, cb) {
  this._bufs.push(buf);
  cb();
};

BlobWriter.prototype._flush = function (cb) {
  this.push(new Blob(this._bufs, {type: 'application/octet-binary'}));
  cb();
};

/**
 * Store Avro values into an Avro-container blob.
 *
 * The returned stream will emit a single value, the blob, when ended.
 *
 */
function createBlobEncoder(schema, opts) {
  var encoder = new containers.streams.BlockEncoder(schema, opts);
  var builder = new BlobWriter();
  encoder.pipe(builder);
  return new stream.Duplex({
    objectMode: true,
    read: function () {
      // Not the fastest implementation, but it will only be called at most
      // once (since the builder only ever emits a single value) so it'll do.
      // It's also likely impractical to create very large blobs.
      var val = builder.read();
      if (val) {
        done(val);
      } else {
        builder.once('readable', done);
      }
      var self = this;
      function done(val) {
        self.push(val || builder.read());
        self.push(null);
      }
    },
    write: function (val, encoding, cb) {
      return encoder.write(val, encoding, cb);
    }
  }).on('finish', function () { encoder.end(); });
}


module.exports = {
  Protocol: protocols.Protocol,
  Type: types.Type,
  assemble: schemas.assemble,
  combine: values.combine,
  createBlobDecoder: createBlobDecoder,
  createBlobEncoder: createBlobEncoder,
  infer: values.infer,
  parse: parse,
  streams: containers.streams,
  types: types.builtins
};
