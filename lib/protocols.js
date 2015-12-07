/* jshint node: true */

'use strict';

var schemas = require('./schemas'),
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

// A often used types.
var METADATA_TYPE = schemas.createType({type: 'map', values: 'bytes'});
var STRING_TYPE = schemas.createType('string');

/**
 * An Avro protocol.
 *
 */
function Protocol(schema) {
  this._name = schemas.resolveNames(schema, undefined, 'protocol').name;

  var opts = {namespace: schema.namespace};
  if (schema.types) {
    // Add all type definitions to `opts.registry`.
    schema.types.forEach(function (obj) { schemas.createType(obj, opts); });
  }

  this._messages = {};
  if (schema.messages) {
    Object.keys(schema.messages).forEach(function (key) {
      this._messages[key] = new Message(key, schema.messages[key], opts);
    }, this);
  }

  this._types = opts.registry;
}

Protocol.prototype.getName = function () { return this._name; };

Protocol.prototype.getTypes = function () { return this._types; };

Protocol.prototype.createEmitter = function (readable, writable, opts) {
  writable = writable || readable;
  var transmitter = new Transmitter(readable, writable, opts);
  return new Emitter(this, transmitter, opts);
};

/**
 * Emitter of things.
 *
 */
function Emitter(protocol, transmitter, opts) {
  events.EventEmitter.call(this);
  this._protocol = protocol;
  this._transmitter = transmitter;

  this._bufferSize = opts && opts.bufferSize || 256;
  this._metadataHook = opts && opts.metadataHook;
  this._id = 0;
  this._callbacks = {};

  this._transmitter.on('data', (function (self, mode) {
    switch (mode) {
      case 'client':
        return function (buf) { self._onResponse(buf); };
      case 'server':
        return function (buf) { self._onRequest(buf); };
      case 'peer':
        return function (buf) {
          var obj = self._decodeMetadata(buf);
          if (obj.value.id > 0) {
            self._onRequest(buf, obj.offset, obj.value);
          } else {
            self._onResponse(buf, obj.offset, obj.value);
          }
        };
      default:
        throw new Error('invalid mode');
    }
  })(this, opts && opts.mode || 'peer'));
}
util.inherits(Emitter, events.EventEmitter);

Emitter.prototype._decodeMetadata = function (buf) {
  var obj = METADATA_TYPE.decode(buf);
  if (obj.value === undefined) {
    this.emit('$error', new Error('invalid metadata'));
    return;
  }
  if (this._metadataHook) {
    obj.value = this._metadataHook(obj.value);
  } else {
    obj.value = {id: obj.value.readInt32BE()};
  }
  return obj;
};

Emitter.prototype.emit = function (name, params, cb) {
  var message = this._protocol._messages[name];
  if (!message) {
    process.nextTick(function () { cb(new Error('unknown message')); });
  }

  var id = this._id++;
  this._pending++;
  var buf = new Buffer(this._bufferSize);
  var pos = 0;
  pos = write(METADATA_TYPE, {id: uintBuffer(id)}, pos);
  pos = write(STRING_TYPE, name, pos);
  pos = write(message._requestType, params, pos);
  if (pos < 0) {
    return; // Error while encoding parameters.
  }

  this._callbacks[id] = {cb: cb, message: message};
  return this._transmitter.write(buf.slice(0, pos));

  function write(type, val, start) {
    try {
      var end = type.encode(buf, val);
    } catch (err) {
      process.nextTick(function () { cb(err); });
      return -1;
    }

    if (end < 0) {
      // TODO: Propagage increased buffer size for later emits.
      var copy = new Buffer(buf.length - end);
      buf.copy(copy, 0, 0, start);
      buf = copy;
      end = type.encode(buf, val);
    }
    return end;
  }
};

Emitter.prototype._onResponse = function (buf, pos, meta) {
  var id = meta.id;
  var info = this._callbacks[meta.id];
  if (!info) {
    this.emit('$error', new Error('orphan response'));
    return;
  }
  delete this._callbacks[-id];

  var message = info.message;
  var isError = buf[pos++];
  var obj;
  if (isError) {
    obj = message._errorType.decode(buf, pos);
  } else {
    obj = message._responseType.decode(buf, pos);
  }

  if (!obj.value) {
    this.emit('$error', new Error('invalid response'));
    return;
  }

  var cb = info.cb;
  if (isError) {
    cb(obj.value);
  } else {
    cb(null, obj.value);
  }
};

Emitter.prototype._onRequest = function (buf, pos, meta) {
  var obj;

  obj = STRING_TYPE.decode(buf, pos);
  var name = obj.value;
  var message = this._protocol._messages[name];
  if (!message) {
    this.emit('$error', new Error('unknown message'));
    return;
  }

  var handlers = this._events && this._events[name]; // Not great to depend on internal.
  if (!handlers) {
    return; // No handlers defined, bypass everything.
  }

  obj = message._requestType.decode(buf, obj.offset);
  if (obj.value === undefined) {
    this.emit('$error', new Error('invalid request'));
  }

  if (typeof handlers == 'function') {
    handlers(null, obj.value, cb);
  } else {
    var i, l;
    for (i = 0, l = handlers.length; i < l; i++) {
      handlers[i](null, obj.value, cb);
    }
  }

  function cb(err, res) {
    // TODO: Transmit response back.
  }
};

Emitter.prototype.close = function () {
  if (!this._closing) {
    this._closing = true;
    if (!this._pending) {
      this._transmitter.end();
    }
  }
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

  var response = attrs.response;
  if (!response) {
    throw new Error('missing response');
  }
  this._responseType = schemas.createType(response, opts);

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
 * Bytes emitter.
 *
 * One to one, OK to enforce this since broadcasting isn't supported in the
 * Avro specification. Also always need both readable and writable since Avro
 * will at least need to do a handshake.
 *
 */
function Transmitter(readable, writable, opts) {
  stream.Duplex.call(this);
  this._readable = readable;
  this._writable = writable;
  this._buf = new Buffer(0);
  this._frames = [];
  this._length = 0; // Current total length of frames.
  this._queue = [];
  this._needPush = false;
  this._frameSize = opts && opts.frameSize || 8192;
  this._endWritable = !!(opts && opts.end);
  this._finished = false;

  this._readable
    .on('data', onData)
    .on('end', onEnd);

  var self = this;
  this.on('finish', function () {
    self._finished = true;
    self._readable.removeListener('data', onData);
    self._readable.removeListener('end', onEnd);
    if (self._endWritable) {
      self._writable.end();
    }
  });

  function onData(buf) {
    self._recv(buf);
  }

  function onEnd() {
    self.push(null);
    if (!self._finished) {
      self.end();
    }
  }
}
util.inherits(Transmitter, stream.Duplex);

Transmitter.prototype._read = function () {
  if (this._queue.length) {
    this.push(this._queue.shift()); // Try LIFO? Pop faster than shift.
  } else {
    this._needPush = true;
  }
};

Transmitter.prototype._write = function (chunk, encoding, cb) {
  var length = chunk.length;
  var start = 0;
  var end;
  do {
    end = start + this._frameSize;
    if (end > length) {
      end = length;
    }
    this._writable.write(uintBuffer(end - start));
    this._writable.write(chunk.slice(start, end));
  } while ((start = end) < length);
  this._writable.write(uintBuffer(0));
  cb(); // TODO: Buffer these writes (checking writable's write return value).
};

Transmitter.prototype._recv = function (chunk) {
  var buf = Buffer.concat([this._buf, chunk]);
  var frameLength;
  while (
    buf.length >= 4 &&
    buf.length >= (frameLength = buf.readInt32BE()) + 4
  ) {
    if (!frameLength) {
      var message = Buffer.concat(this._frames, this._length);
      this._length = 0;
      this._frames = [];
      if (this._needPush) {
        this._needPush = false;
        this.push(message);
      } else {
        this._queue.push(message);
      }
    } else {
      this._length += frameLength;
      this._frames.push(buf.slice(4, frameLength + 4));
    }
    buf = buf.slice(frameLength + 4);
  }
  this._buf = buf;
};

// Convert size to big endian integer.
function uintBuffer(n) {
  var buf = new Buffer(4);
  buf.writeInt32BE(n);
  return buf;
}

module.exports = {
  HANDSHAKE_REQUEST_TYPE: HANDSHAKE_REQUEST_TYPE,
  HANDSHAKE_RESPONSE_TYPE: HANDSHAKE_RESPONSE_TYPE,
  Message: Message,
  Protocol: Protocol,
  Transmitter: Transmitter
};
