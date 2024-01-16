'use strict';

/**
 * Main browserify entry point.
 *
 * This version of the entry point adds a couple browser-specific utilities to
 * read and write blobs.
 */

let containers = require('../../lib/containers'),
    buffer = require('buffer'),
    stream = require('stream');

let Buffer = buffer.Buffer;

/** Transform stream which lazily reads a blob's contents. */
class BlobReader extends stream.Readable {
  constructor (blob, opts) {
    super();
    opts = opts || {};

    this._batchSize = opts.batchSize || 65536;
    this._blob = blob;
    this._pos = 0;
  }

  _read () {
    let pos = this._pos;
    if (pos >= this._blob.size) {
      this.push(null);
      return;
    }

    this._pos += this._batchSize;
    let blob = this._blob.slice(pos, this._pos, this._blob.type);
    let reader = new FileReader();
    let self = this;
    reader.addEventListener('loadend', function cb(evt) {
      reader.removeEventListener('loadend', cb, false);
      if (evt.error) {
        self.emit('error', evt.error);
      } else {
        self.push(Buffer.from(reader.result));
      }
    }, false);
    reader.readAsArrayBuffer(blob);
  }
}

/** Transform stream which builds a blob from all data written to it. */
class BlobWriter extends stream.Transform {
  constructor () {
    super({readableObjectMode: true});
    this._bufs = [];
  }

  _transform (buf, encoding, cb) {
    this._bufs.push(buf);
    cb();
  }

  _flush (cb) {
    this.push(new Blob(this._bufs, {type: 'application/octet-binary'}));
    cb();
  }
}

/** Read an Avro-container stored as a blob. */
function createBlobDecoder(blob, opts) {
  return new BlobReader(blob).pipe(new containers.streams.BlockDecoder(opts));
}

/**
 * Store Avro values into an Avro-container blob.
 *
 * The returned stream will emit a single value, the blob, when ended.
 */
function createBlobEncoder(schema, opts) {
  let encoder = new containers.streams.BlockEncoder(schema, opts);
  let builder = new BlobWriter();
  encoder.pipe(builder);
  return new stream.Duplex({
    objectMode: true,
    read: function () {
      // Not the fastest implementation, but it will only be called at most
      // once (since the builder only ever emits a single value) so it'll do.
      // It's also likely impractical to create very large blobs.
      let val = builder.read();
      if (val) {
        done(val);
      } else {
        builder.once('readable', done);
      }
      let self = this;
      function done(val) {
        self.push(val || builder.read());
        self.push(null);
      }
    },
    write: function (val, encoding, cb) {
      return encoder.write(val, encoding, cb);
    }
  }).on('finish', () => { encoder.end(); });
}


module.exports = {
  createBlobDecoder,
  createBlobEncoder,
  streams: containers.streams
};
