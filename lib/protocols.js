/* jshint node: true */

// TODO: Default event when request is not being listened to.
// TODO: Rename transports to `Dedicated` and the other to `Shared`?
// TODO: Add other transport subclass which doesn't assume streams.
// TODO: Rename Emitter to MessageEmitter.

'use strict';

var schemas = require('./schemas'),
    utils = require('./utils'),
    events = require('events'),
    stream = require('stream'),
    util = require('util');

var HANDSHAKE_REQUEST_TYPE = schemas.createType({
  name: 'org.apache.avro.ipc.HandshakeRequest',
  type: 'record',
  fields: [
    {name: 'clientHash', type: {name: 'MD5', type: 'fixed', size: 16}},
    {name: 'clientProtocol', type: ['null', 'string']},
    {name: 'serverHash', type: 'MD5'},
    {name: 'meta', type: ['null', {type: 'map', values: 'bytes'}]}
  ]
});

var HANDSHAKE_RESPONSE_TYPE = schemas.createType({
  name: 'org.apache.avro.ipc.HandshakeResponse',
  type: 'record',
  fields: [
    {
      name: 'match',
      type: {
        name: 'HandshakeMatch',
        type: 'enum',
        symbols: ['BOTH', 'CLIENT', 'NONE']
      }
    },
    {name: 'serverProtocol', type: ['null', 'string']},
    {
      name: 'serverHash',
      type: ['null', {name: 'MD5', type: 'fixed', size: 16}]
    },
    {name: 'meta', type: ['null', {type: 'map', values: 'bytes'}]}
  ]
});

// Often used types.
var BOOLEAN_TYPE = schemas.createType('boolean');
var STRING_TYPE = schemas.createType('string');

var Tap = utils.Tap;

/**
 * An Avro protocol.
 *
 */
function Protocol(schema) {
  this._name = schemas.resolveNames(schema, undefined, 'protocol').name;

  var opts = {namespace: schema.namespace};
  if (schema.types) {
    schema.types.forEach(function (obj) { schemas.createType(obj, opts); });
  }

  this._messages = {};
  if (schema.messages) {
    Object.keys(schema.messages).forEach(function (key) {
      this._messages[key] = new Message(key, schema.messages[key], opts);
    }, this);
  }
}

Protocol.prototype.getName = function () { return this._name; };

Protocol.prototype.createEmitter = function (opts) {
  opts = opts || {};

  var emitter = new Emitter(this, opts);
  var transport = opts.transport;
  if (transport) {
    if (
      !(transport instanceof Transport) &&
      transport instanceof stream.Duplex
    ) {
      // Convenience for common use-case.
      transport = new StreamTransport(transport);
    }
    emitter.setTransport(transport);
  }
  return emitter;
};

/**
 * Emitter of things.
 *
 */
function Emitter(protocol, opts) {
  events.EventEmitter.call(this);
  opts = opts || {};

  this._protocol = protocol;
  this._transport = null;
  this._handlers = {};
  this._pending = {};
  this._id = 1;

  var Type = opts.IdType || IdType;
  this._idType = new Type({type: 'map', values: 'bytes'});
  this._bufferSize = opts.bufferSize || 256;
  this._defaultHandler = opts.defaultHandler;
  this._mode = opts.mode || 'peer';
  this._onTranportData = (function (self) {
    switch (self._mode) {
      case 'client':
        return function (obj) {
          var tap = new Tap(obj.data);
          var id = self._idType._read(tap) || obj.hint;
          self._onResponse(id, tap);
        };
      case 'server':
        return function (obj) {
          var tap = new Tap(obj.data);
          var id = self._idType._read(tap) || obj.hint;
          self._onRequest(-id, tap);
        };
      case 'peer':
        return function (obj) {
          var tap = new Tap(obj.data);
          var id = self._idType._read(tap) || obj.hint;
          if (id > 0) {
            self._onResponse(id, tap);
          } else {
            self._onRequest(-id, tap);
          }
        };
      default:
        throw new Error('invalid mode');
    }
  })(this);
}
util.inherits(Emitter, events.EventEmitter);

Emitter.prototype.setTransport = function (transport) {
  if (this._transport) {
    this._transport.removeListener('data', this._onTransportData);
    this._transport.release();
  }

  this._transport = transport;
  if (transport) {
    transport.on('data', this._onTranportData);
  }
};

Emitter.prototype.emitMessage = function (name, params, cb) {
  if (this._mode === 'server') {
    process.nextTick(function () { cb(new Error('server cannot emit')); });
    return;
  }

  var message = this._protocol._messages[name];
  if (!message) {
    process.nextTick(function () { cb(new Error('unknown message')); });
    return;
  }

  var id = this._id++;
  var tap = new Tap(new Buffer(this._bufferSize));
  if (
    safeWrite(tap, this._idType, -id, cb) &&
    safeWrite(tap, STRING_TYPE, name, cb) &&
    safeWrite(tap, message._requestType, params, cb)
  ) {
    this._pending[id] = {cb: cb, message: message};
    return this._transport.write({data: tap.buf.slice(0, tap.pos), hint: -id});
  } else {
    // Invalid data.
    return true;
  }
};

Emitter.prototype.onMessage = function (name, handler, force) {
  if (this._mode === 'client') {
    throw new Error('client cannot listen');
  }

  var message = this._protocol._messages[name];
  if (!message) {
    throw new Error('unknown message');
  }

  if (this._handlers[name] && !force) {
    throw new Error('already listening');
  }
  this._handlers[name] = handler;
};

Emitter.prototype._onResponse = function (id, tap) {
  var info = this._pending[id];
  if (!info) {
    this.emit('error', new Error('orphan response'));
    return;
  }
  delete this._pending[id];

  var message = info.message;
  try {
    var err = null;
    var res;
    if (tap.readBoolean()) {
      err = message._errorType._read(tap);
    } else {
      res = message._responseType._read(tap);
    }
  } catch (err) {
    this.emit('error', new Error('invalid res'));
    return;
  }

  if (!tap.isValid()) {
    this.emit('error', new Error('truncated res'));
    return;
  }
  info.cb(err, res); // TODO: If `error` is null, this might cause surprises.
};

Emitter.prototype._onRequest = function (id, tap) {
  // TODO: Handshake.
  var self = this;
  var name = STRING_TYPE._read(tap);
  var message = this._protocol._messages[name]; // Not undefined (handshake).
  var handler = this._handlers[name];
  if (!handler) {
    cb('not implemented');
    return;
  }

  try {
    var req = message._requestType._read(tap);
  } catch (err) {
    // This shouldn't happen.
    cb(err.message);
    this.emit('error', err);
    return;
  }
  handler(req, cb);

  function cb(err, res) {
    var tap = new Tap(new Buffer(self._bufferSize));
    var noError = err === null;
    if (
      safeWrite(tap, self._idType, id, errCb) &&
      safeWrite(tap, BOOLEAN_TYPE, !noError, errCb) &&
      noError ?
        safeWrite(tap, message._responseType, res, errCb) :
        safeWrite(tap, message._errorType, err, errCb)
    ) {
      // This shouldn't happen.
      self._transport.write({data: tap.buf.slice(0, tap.pos), hint: id});
    }

    function errCb(err) {
      self.emit('error', err);
    }
  }
};

/**
 * Object duplex stream. {metadata, contents}
 *
 * Transports should not be shared. They can only be reused after having been
 * released.
 *
 */
function Transport(opts) {
  stream.Duplex.call(this, {objectMode: true});
  this._frameSize = opts && opts.frameSize || 8192;
  this._queue = [];
  this._needPush = false;
}
util.inherits(Transport, stream.Duplex);

Transport.prototype._read = function () {
  if (this._queue.length) {
    this.push(this._queue.shift()); // Try LIFO? Pop faster than shift.
  } else {
    this._needPush = true;
  }
};

// obj == {data, hint}
Transport.prototype._write = function (obj, encoding, cb) {
  var bufs = [];
  var buf = obj.data;
  var length = buf.length;
  var start = 0;
  var end;
  do {
    end = start + this._frameSize;
    if (end > length) {
      end = length;
    }
    bufs.push(intBuffer(end - start));
    bufs.push(buf.slice(start, end));
  } while ((start = end) < length);

  bufs.push(intBuffer(0));
  this._send(bufs, obj.hint, cb);
};

// This function should be called when data arrives.
Transport.prototype._recv = function (hint) {
  var self = this;
  var length = 0;
  var frames = [];
  var buf = new Buffer(0);

  return function (chunk) {
    buf = Buffer.concat([buf, chunk]);
    var frameLength, obj;
    while (
      buf.length >= 4 &&
      buf.length >= (frameLength = buf.readInt32BE()) + 4
    ) {
      if (frameLength) {
        length += frameLength;
        frames.push(buf.slice(4, frameLength + 4));
      } else {
        obj = {data: Buffer.concat(frames, length), hint: hint};
        length = 0;
        frames = [];
        if (self._needPush) {
          self._needPush = false;
          self.push(obj);
        } else {
          self._queue.push(obj);
        }
      }
      buf = buf.slice(frameLength + 4);
    }
  };
};

// Called with a list of frames and write's callback.
Transport.prototype._send = utils.abstractFunction;

// Release underlying resources.
Transport.prototype.release = function () {};


/**
 * Bytes emitter.
 *
 * One to one, OK to enforce this since broadcasting isn't supported in the
 * Avro specification. Also always need both readable and writable since Avro
 * will at least need to do a handshake.
 *
 */
function StreamTransport(readable, writable, opts) {
  Transport.call(this, opts);
  this._readable = readable;
  this._writable = writable;
  this._end = !!(opts && opts.end);

  var self = this;
  this._onData = this._recv(0); // No hint.
  this._onEnd = function () { self.push(null); };
  this._readable
    .on('data', this._onData)
    .on('end', this._onEnd);
  this.on('finish', function () {
    self._writable.end();
    self.release();
  });
}
util.inherits(StreamTransport, Transport);

StreamTransport.prototype.release = function () {
  this._readable.removeListener('data', this._onData);
  this._readable.removeListener('end', this._onEnd);
  this._readable = null;
  this._writable = null;
};

StreamTransport.prototype._send = function (bufs, hint, cb) {
  var noDrain = true;
  var i, l;
  for (i = 0, l = bufs.length; i < l; i++) {
    noDrain = this._writable.write(bufs[i]);
  }
  if (this._end) {
    cb();
    this.end();
  } else if (noDrain) {
    cb();
  } else {
    this._writable.once('drain', cb);
  }
};

/**
 *
 * factory(cb(readable)): writable
 *
 * Http:
 *
 *  function (cb) {
 *    return http.request(opts, cb);
 *  });
 *
 */
function CallbackTransport(factory, opts) {
  Transport.call(this, opts);
  this._factory = factory;
  this.on('finish', function () {
    // We don't need to check for pending requests since the write callback is
    // delayed until responses have ended and finish waits for all such
    // callbacks to have been called.
    this.push(null);
  });
}
util.inherits(CallbackTransport, Transport);

CallbackTransport.prototype._send = function (bufs, hint, cb) {
  var self = this;
  var writable = this._factory(function (readable) {
    var onData = self._recv(hint);
    readable
      .on('data', onData)
      .on('end', cb)
      .on('error', function (err) {
        readable.removeListener('data', onData);
        readable.removeListener('end', cb);
        self.emit('error', err);
      });
  });
  var i, l;
  for (i = 0, l = bufs.length; i < l; i++) {
    writable.write(bufs[i]);
  }
  writable.end();
};

// Helpers.

/**
 * An Avro message.
 *
 */
function Message(name, attrs, opts) {
  this._requestType = schemas.createType({
    name: name,
    type: 'record',
    fields: attrs.request
  }, opts);

  var res = attrs.response;
  if (!res) {
    throw new Error('missing res');
  }
  this._responseType = schemas.createType(res, opts);

  var errors = attrs.errors || [];
  errors.unshift('string');
  this._errorType = schemas.createType(errors, opts);

  this._oneWay = !!attrs['one-way'];
  if (this._oneWay) {
    if (
      !(this._responseType instanceof schemas.types.NullType) ||
      errors.length > 1
    ) {
      throw new Error('unapplicable one-way parameter');
    }
  }
}

/**
 * Default ID generator, using Avro messages' metadata field.
 *
 * This can be overridden to read or write arbitrary metadata. Note that the
 * message contents are (intentionally) not available when updating this
 * metadata.
 *
 */
function IdType(attrs, opts) {
  schemas.types.LogicalType.call(this, attrs, opts);
}
util.inherits(IdType, schemas.types.LogicalType);
IdType.prototype._fromValue = function (val) { return val.id.readInt32BE(); };
IdType.prototype._toValue = function (any) { return {id: intBuffer(any)}; };

// Convert size to big endian integer.
function intBuffer(n) {
  var buf = new Buffer(4);
  buf.writeInt32BE(n);
  return buf;
}

// Write and maybe resize.
function safeWrite(tap, type, val, cb) {
  var pos = tap.pos;
  try {
    type._write(tap, val);
  } catch (err) {
    process.nextTick(function () { cb(err); });
    return false;
  }

  if (!tap.isValid()) {
    // TODO: Use new tap.buf length to resize buffer?
    var buf = new Buffer(tap.pos);
    tap.buf.copy(buf, 0, 0, pos);
    tap.buf = buf;
    tap.pos = pos;
    type._write(tap, val);
  }
  return true;
}


module.exports = {
  HANDSHAKE_REQUEST_TYPE: HANDSHAKE_REQUEST_TYPE,
  HANDSHAKE_RESPONSE_TYPE: HANDSHAKE_RESPONSE_TYPE,
  Message: Message,
  Protocol: Protocol,
  Emitter: Emitter,
  transports: {
    StreamTransport: StreamTransport,
    CallbackTransport: CallbackTransport
  }
};
