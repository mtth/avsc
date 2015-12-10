/* jshint node: true */

// TODO: Default event when request is not being listened to.
// TODO: Allow passing options to message encoder.
// TODO: Make message properties "public" (data object).

'use strict';

var schemas = require('./schemas'),
    utils = require('./utils'),
    events = require('events'),
    stream = require('stream'),
    util = require('util');


var BOOLEAN_TYPE = schemas.createType('boolean');

var STRING_TYPE = schemas.createType('string');

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

Protocol.prototype.createClient = function (transport, opts) {
  if (typeof transport == 'function') {
    return new StatelessClient(this, transport, opts);
  } else if (transport instanceof stream.Duplex) {
    return new StatefulClient(this, transport, transport, opts);
  } else {
    return new StatefulClient(
      this, transport.readable, transport.writable, opts
    );
  }
};

Protocol.prototype.createServer = function (opts) {
  return new Server(this, opts);
};


/**
 * Base client class.
 *
 */
function Client(protocol, opts) {
  events.EventEmitter.call(this);
  opts = opts || {};

  var Type = opts.IdType || IdType;
  this._idType = new Type({type: 'map', values: 'bytes'});
  this._bufferSize = opts.bufferSize || 256;
  this._protocol = protocol;
  this._id = 1;
}
util.inherits(Client, events.EventEmitter);

Client.prototype._decodeResponse = function (message, tap, cb) {
  var err = null;
  var res = null;
  try {
    if (tap.readBoolean()) {
      err = message._errorType._read(tap);
    } else {
      res = message._responseType._read(tap);
    }
  } catch (err_) {
    cb(err_);
    return;
  }
  if (!tap.isValid()) {
    cb(new Error('truncated response'));
    return;
  }
  cb(err, res);
};

Client.prototype._encodeRequest = function (name, params, cb) {
  var message = this._protocol._messages[name];
  if (!message) {
    cb(new Error('unknown message'));
    return;
  }
  var id = this._id++;
  var tap = new Tap(new Buffer(this._bufferSize));
  if (
    safeWrite(tap, this._idType, -id, cb) &&
    safeWrite(tap, STRING_TYPE, name, cb) &&
    safeWrite(tap, message._requestType, params, cb)
  ) {
    cb(null, tap.buf.slice(0, tap.pos), message, id);
  }
};


/**
 * Emitter of messages.
 *
 */
function StatelessClient(protocol, writableFactory, opts) {
  Client.call(this, protocol, opts);
  this._writableFactory = writableFactory;
}
util.inherits(StatelessClient, Client);

StatelessClient.prototype.emitMessage = function (name, params, cb) {
  var self = this;
  this._encodeRequest(name, params, function (err, buf, message) {
    if (err) {
      process.nextTick(function () { cb(avroError(err)); });
      return;
    }
    var encoder = new MessageEncoder();
    var writable = self._writableFactory(function (readable) {
      readable
        .pipe(new MessageDecoder())
        .on('data', function (buf) {
          var tap = new Tap(buf);
          self._idType._read(tap); // Skip metadata.
          self._decodeResponse(message, tap, function (err, res) {
            cb(avroError(err), res);
          });
        });
    });
    encoder.pipe(writable);
    encoder.end(buf);
  });
};


/**
 * Used when streams are passed in. These are assumed to represent a persistent
 * connection.
 *
 */
function StatefulClient(protocol, readable, writable, opts) {
  Client.call(this, protocol, opts);
  this._pending = {};
  this._encoder = new MessageEncoder();
  this._writable = writable;
  this._encoder.pipe(this._writable);

  var self = this;
  this._readable = readable
    .pipe(new MessageDecoder())
    .on('data', function (buf) {
      var tap = new Tap(buf);
      var id = self._idType._read(tap);
      var info = self._pending[id];
      if (info === undefined) {
        self.emit('error', new Error('orphan response'));
        return;
      }
      delete self._pending[id];
      self._decodeResponse(info.message, tap, function (err, res) {
        info.cb(avroError(err), res);
      });
    })
    .on('end', function () {}); // TODO: Close client.
}
util.inherits(StatefulClient, Client);

StatefulClient.prototype.emitMessage = function (name, params, cb) {
  var self = this;
  this._encodeRequest(name, params, function (err, buf, message, id) {
    if (err) {
      process.nextTick(function () { self.emit('error', err); });
      return;
    }
    self._pending[id] = {message: message, cb: cb};
    self._encoder.write(buf);
  });
};

/**
 * Receiver.
 *
 */
function Server(protocol, opts) {
  events.EventEmitter.call(this);
  opts = opts || {};

  var Type = opts.IdType || IdType;
  this._idType = new Type({type: 'map', values: 'bytes'});
  this._bufferSize = opts.bufferSize || 256;
  this._defaultHandler = opts.defaultHandler;
  this._protocol = protocol;
  this._handlers = {};
}
util.inherits(Server, events.EventEmitter);

Server.prototype.addTransport = function (transport) {
  var self = this;

  var channel = new Channel();
  if (typeof transport == 'function') {
    channel.isStateful = false;
    channel.setReadable(transport(function (writable) {
      channel.setWritable(writable);
      if (channel.buf) {
        channel.writable.end(channel.buf);
      }
    }));
  } else if (transport instanceof stream.Duplex) {
    channel.setReadable(transport).setWritable(transport);
  } else {
    channel.setReadable(transport.readable).setWritable(transport.writable);
  }

  channel.readable.on('data', function (buf) {
    var tap = new Tap(buf);
    // TODO: handshake.
    var id = -self._idType._read(tap);
    var name = STRING_TYPE._read(tap);
    var message = self._protocol._messages[name];
    try {
      var req = message._requestType._read(tap);
    } catch (err) {
      // This shouldn't happen.
      self.emit('error', err);
      return;
    }

    var handler = self._handlers[name];
    if (!handler) {
      cb({string: 'not implemented'});
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
        var buf = tap.buf.slice(0, tap.pos);
        var writable = channel.writable;
        if (writable) {
          writable.write(buf);
        } else {
          writable.buf = buf;
        }
      }

      function errCb(err) {
        self.emit('error', err);
      }
    }
  }).on('end', function () {}); // TODO

  return this;
};

Server.prototype.onMessage = function (name, handler, force) {
  var message = this._protocol._messages[name];
  if (!message) {
    throw new Error('unknown message');
  }
  if (this._handlers[name] && !force) {
    throw new Error('already listening');
  }
  this._handlers[name] = handler;
  return this;
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
 * Server transport abstraction.
 *
 */
function Channel() {
  this.isStateful = true;
  this.readable = null;
  this.writable = null;
  this.buf = null; // Buffer pending write.
}

Channel.prototype.setReadable = function (readable) {
  this.readable = readable.pipe(new MessageDecoder());
  return this;
};

Channel.prototype.setWritable = function (writable) {
  this.writable = new MessageEncoder();
  this.writable.pipe(writable);
  return this;
};

/**
 * Framing stream.
 *
 */
function MessageEncoder(opts) {
  stream.Transform.call(this);
  this._frameSize = opts && opts.frameSize || 8192;
}
util.inherits(MessageEncoder, stream.Transform);

MessageEncoder.prototype._transform = function (buf, encoding, cb) {
  // TODO: Optimize this by avoiding the extra copy.
  var frames = [];
  var length = buf.length;
  var start = 0;
  var end;
  do {
    end = start + this._frameSize;
    if (end > length) {
      end = length;
    }
    frames.push(intBuffer(end - start));
    frames.push(buf.slice(start, end));
  } while ((start = end) < length);
  frames.push(intBuffer(0));
  cb(null, Buffer.concat(frames));
};

/**
 * Unframing stream.
 *
 */
function MessageDecoder() {
  stream.Transform.call(this);
  this._bufs = [];
  this._length = 0;
  this._buf = new Buffer(0);
  this.on('finish', function () { this.push(null); });
}
util.inherits(MessageDecoder, stream.Transform);

MessageDecoder.prototype._transform = function (buf, encoding, cb) {
  buf = Buffer.concat([this._buf, buf]);
  var frameLength;
  while (
    buf.length >= 4 &&
    buf.length >= (frameLength = buf.readInt32BE()) + 4
  ) {
    if (frameLength) {
      this._bufs.push(buf.slice(4, frameLength + 4));
      this._length += frameLength;
    } else {
      this.push(Buffer.concat(this._bufs), this._length);
      this._length = 0;
      this._bufs = [];
    }
    buf = buf.slice(frameLength + 4);
  }
  this._buf = buf;
  cb();
};

MessageDecoder.prototype._flush = function () {
  if (this._length || this._buf.length) {
    this.emit('error', 'trailing data');
  }
};

/**
 * Default ID generator, using Avro messages' metadata field.
 *
 * This is required for stateful clients to work.
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

/**
 * Convert a number into a buffer containing its big endian representation.
 *
 */
function intBuffer(n) {
  var buf = new Buffer(4);
  buf.writeInt32BE(n);
  return buf;
}

/**
 * Write and maybe resize.
 *
 */
function safeWrite(tap, type, val, cb) {
  var pos = tap.pos;
  try {
    type._write(tap, val);
  } catch (err) {
    process.nextTick(function () { cb(err); });
    return false;
  }

  if (!tap.isValid()) {
    var buf = new Buffer(tap.pos);
    tap.buf.copy(buf, 0, 0, pos);
    tap.buf = buf;
    tap.pos = pos;
    type._write(tap, val);
  }
  return true;
}

/**
 * Convert error into valid format for Avro.
 *
 */
function avroError(err) {
  return err instanceof Error ? {string: '' + err} : err;
}


module.exports = {
  HANDSHAKE_REQUEST_TYPE: HANDSHAKE_REQUEST_TYPE,
  HANDSHAKE_RESPONSE_TYPE: HANDSHAKE_RESPONSE_TYPE,
  Message: Message,
  Protocol: Protocol,
  Client: Client,
  Server: Server,
  streams: {
    MessageDecoder: MessageDecoder,
    MessageEncoder: MessageEncoder
  }
};
