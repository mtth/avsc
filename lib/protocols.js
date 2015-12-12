/* jshint node: true */

// TODO: Allow passing options to message encoder.
// TODO: Proxy all channel errors to both the server and the response.
// TODO: One way handling.
// TODO: Make channels keep a reference to the protocol rather than the server.

'use strict';

var schemas = require('./schemas'),
    utils = require('./utils'),
    assert = require('assert'),
    events = require('events'),
    stream = require('stream'),
    util = require('util');


var BOOLEAN_TYPE = schemas.createType('boolean');

var STRING_TYPE = schemas.createType('string');

var HANDSHAKE_REQUEST_TYPE = schemas.createType({
  namespace: 'org.apache.avro.ipc',
  name: 'HandshakeRequest',
  type: 'record',
  fields: [
    {name: 'clientHash', type: {name: 'MD5', type: 'fixed', size: 16}},
    {name: 'clientProtocol', type: ['null', 'string']},
    {name: 'serverHash', type: 'org.apache.avro.ipc.MD5'},
    {name: 'meta', type: ['null', {type: 'map', values: 'bytes'}]}
  ]
});

var HANDSHAKE_RESPONSE_TYPE = schemas.createType({
  namespace: 'org.apache.avro.ipc',
  name: 'HandshakeResponse',
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
var f = util.format;

/**
 * An Avro protocol.
 *
 */
function Protocol(attrs, opts) {
  opts = opts || {};

  this._name = attrs.protocol;
  if (!this._name) {
    throw new Error('missing protocol name');
  }
  var namespace = attrs.namespace;
  if (namespace && !~this._name.indexOf('.')) {
    this._name = f('%s.%s', namespace, this._name);
  }

  opts.namespace = namespace;
  if (attrs.types) {
    attrs.types.forEach(function (obj) { schemas.createType(obj, opts); });
  }

  this._messages = {};
  if (attrs.messages) {
    Object.keys(attrs.messages).forEach(function (key) {
      this._messages[key] = new Message(key, attrs.messages[key], opts);
    }, this);
  }

  this._types = [];
  Object.keys(opts.registry || {}).forEach(function (name) {
    var type = opts.registry[name];
    if (type.getName() && type.getName(true) !== 'message') {
      this._types.push(type);
    }
  }, this);

  this._hashString = utils.getFingerprint(this.toString()).toString('binary');
  this._resolvers = {};
}

Protocol.prototype._addResolvers = function (remoteProtocol) {
  if (remoteProtocol._hashString === this._hashString) {
    return; // Protocols are equal, no need to store any resolvers.
  }

  var resolvers = {};
  var localMessages = this._messages;
  var remoteMessages = remoteProtocol._messages;
  // Add resolvers for clients (these are only relevant for local messages).
  if (localMessages) {
    Object.keys(localMessages).forEach(function (name) {
      var lm = localMessages[name];
      var rm = remoteMessages[name];
      resolvers[name] = {
        _responseType: lm._responseType.createResolver(rm._responseType),
        _errorType: lm._errorType.createResolver(rm._errorType)
      };
    });
  }
  // Add request resolvers for servers (only relevant for remote messages now).
  if (remoteMessages) {
    Object.keys(remoteMessages).forEach(function (name) {
      var lm = localMessages[name];
      var rm = remoteMessages[name];
      if (!resolvers[name]) {
        resolvers[name] = {};
      }
      var resolver = lm._responseType.createResolver(rm._responseType);
      resolvers[name]._requestType = resolver;
    });
  }

  this._resolvers[remoteProtocol._hashString] = resolvers;
};

Protocol.prototype._canResolve = function (hashString) {
  return hashString === this._hashString || !!this._resolvers[hashString];
};

Protocol.prototype._getResolvers = function (hashString, message) {
  if (hashString === this._hashString) {
    return message;
  }
  var allResolvers = this._resolvers[hashString];
  return allResolvers && allResolvers[message._name];
};

Protocol.prototype.getName = function () { return this._name; };

Protocol.prototype.getHash = function () {
  return new Buffer(this._hashString, 'binary');
};

Protocol.prototype.getMessage = function (name) {
  return this._messages[name];
};

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

Protocol.prototype.toString = function () {
  return schemas.stringify({
    protocol: this._name,
    types: this._types,
    messages: this._messages
  });
};

Protocol.prototype.inspect = function () {
  return f('<Protocol %j>', this._name);
};

/**
 * Base client class.
 *
 * A client is a message emitter. See below for the two available variants.
 *
 */
function Client(protocol, opts) {
  events.EventEmitter.call(this);
  opts = opts || {};

  this._protocol = protocol;

  var Type = opts.IdType || IdType;
  this._idType = new Type({type: 'map', values: 'bytes'});
  this._bufferSize = opts.bufferSize || 256;
  this._protocolOpts = opts.protocolOpts;
  this._includeProtocol = !!opts.includeProtocol;
  this._serverHashString = opts.serverHash ?
    opts.serverHash.toString('binary') :
    protocol._hashString; // Sane default.
  this._id = 1;
}
util.inherits(Client, events.EventEmitter);

Client.prototype._encodeHandshake = function (tap) {
  var lp = this._protocol;
  var handshake = {
    clientHash: lp.getHash(),
    clientProtocol: this._includeProtocol ? {string: lp.toString()} : null,
    serverHash: new Buffer(this._serverHashString, 'binary'),
    meta: null
  };
  safeWrite(tap, HANDSHAKE_REQUEST_TYPE, handshake);
};

Client.prototype._decodeHandshake = function (tap) {
  var handshake = HANDSHAKE_RESPONSE_TYPE._read(tap);
  if (handshake.match === 'BOTH') {
    return true; // We already have the server's protocol, skip the rest.
  }

  assert(handshake.serverProtocol);
  assert(!this._protocol._canResolve(handshake.serverHash.toString('binary')));

  var rp = new Protocol(
    JSON.parse(handshake.serverProtocol.string),
    this._protocolOpts
  );
  assert(rp.getHash().equals(handshake.serverHash['org.apache.avro.ipc.MD5']));
  this._protocol._addResolvers(rp);
  this._serverHashString = rp._hashString;
  return handshake.match === 'CLIENT';
};

Client.prototype._encodeRequest = function (tap, message, params) {
  safeWrite(tap, STRING_TYPE, message._name);
  safeWrite(tap, message._requestType, params);
  return tap.getValue();
};

Client.prototype._decodeResponse = function (tap, message) {
  var protocol = this._protocol;
  var resolvers = protocol._getResolvers(this._serverHashString, message);

  var args = [null, null];
  if (tap.readBoolean()) {
    args[0] = resolvers._errorType._read(tap);
  } else {
    args[1] = resolvers._responseType._read(tap);
  }

  if (!tap.isValid()) {
    throw new Error('truncated buffer');
  }
  return args;
};

/**
 * Factory-based client.
 *
 * This client doesn't keep a persistent connection to the server and requires
 * prepending a handshake to each message emitted. Usage examples include
 * talking to an HTTP server (where the factory returns an HTTP request).
 *
 * Since each message will use its own writable/readable stream pair, the
 * advandage of this client is that it is able to keep track of which response
 * corresponds to each request without relying on messages' metadata. In
 * particular, this means these clients are compatible with any server
 * implementation.
 *
 */
function StatelessClient(protocol, writableFactory, opts) {
  Client.call(this, protocol, opts);
  this._writableFactory = writableFactory;
  this._pending = 0;
}
util.inherits(StatelessClient, Client);

StatelessClient.prototype.emitMessage = function (name, params, cb) {
  if (this._destroyed) {
    asyncAvroError('client destroyed', cb);
    return;
  }

  var self = this;
  var tap = new Tap(new Buffer(this._bufferSize));
  var message = this._protocol.getMessage(name);
  if (!message) {
    asyncAvroError('unknown message', cb);
    return;
  }

  var id = this._id++;
  this._encodeHandshake(tap);
  try {
    safeWrite(tap, this._idType, -id);
    this._encodeRequest(tap, message, params);
  } catch (err) {
    asyncAvroError(err, cb);
    return;
  }

  var encoder = new MessageEncoder();
  var writable = self._writableFactory(function (readable) {
    readable
      .pipe(new MessageDecoder())
      .once('data', function (buf) {
        if (!--self.pending && self._destroyed) {
          self.emit('end');
        }

        var tap = new Tap(buf);
        try {
          if (!self._decodeHandshake(tap)) {
            // Handshake failed, server doesn't have client protocol.
            assert(!self._includeProtocol);
            self._includeProtocol = true;
            self.emitMessage(name, params, cb);
          } else {
            self._idType._read(tap); // Skip metadata.
            cb.apply(undefined, self._decodeResponse(tap, message));
          }
        } catch (err) {
          asyncAvroError(err, cb);
        }
      });

    this._pending++;
    encoder.pipe(writable);
    encoder.end(tap.getValue());
  });
};

StatelessClient.prototype.destroy = function () {
  this._destroyed = true;
  if (!this._pending) {
    this.emit('end');
  }
};

/**
 * Multiplexing client.
 *
 * These clients reuse the same streams (both readable and writable) for all
 * messages. This avoid a lot of overhead (e.g. creating new connections,
 * re-issuing handshakes) but requires the server to include compatible
 * metadata in each response (namely forwarding each request's ID into its
 * response).
 *
 * A custom metadata format can be specified via the `idType` option. The
 * default is compatible with this package's default server implementation.
 *
 */
function StatefulClient(protocol, readable, writable, opts) {
  Client.call(this, protocol, opts);
  this._readable = readable;
  this._writable = writable;
  this._started = false;
  this._destroyed = false;
  this._pending = {};

  var self = this;
  this._decoder = this._readable
    .pipe(new MessageDecoder())
    .on('error', function (err) { self.emit('error', err); })
    .on('data', onHandshakeData)
    .on('end', function () {
      if (!self._noPending()) {
        self.emit('error', new Error('interrupted'));
      } else {
        self.emit('end');
      }
    });

  this._encoder = new MessageEncoder();
  this._encoder.pipe(this._writable);
  emitHandshake();

  function emitHandshake() {
    var tap = new Tap(new Buffer(self._bufferSize));
    self._encodeHandshake(tap);
    self._encoder.write(tap.getValue());
  }

  function onHandshakeData(buf) {
    var tap = new Tap(buf);
    try {
      var handshakeSuccessful = self._decodeHandshake(tap);
    } catch (err) {
      self.emit('error', err);
      self.destroy();
      return;
    }
    if (handshakeSuccessful) {
      self._decoder
        .removeListener('data', onHandshakeData)
        .on('data', onMessageData);
      self._started = true;
      self.emit('start'); // Send any pending messages.
    } else {
      self._includeProtocol = true;
      emitHandshake();
    }
  }

  function onMessageData(buf) {
    var tap = new Tap(buf);
    try {
      var id = self._idType._read(tap);
    } catch (err) {
      self.emit('error', new Error('invalid metadata'));
      return;
    }
    var info = self._pending[id];
    if (info === undefined) {
      self.emit('error', new Error('orphan response'));
      return;
    }
    delete self._pending[id];
    if (self._destroyed && self._noPending()) {
      self.emit('end');
    }
    try {
      info.cb.apply(undefined, self._decodeResponse(tap, info.message));
    } catch (err) {
      asyncAvroError(err, info.cb);
    }
  }
}
util.inherits(StatefulClient, Client);

StatefulClient.prototype._noPending = function () {
  // Not worth caching redundant state for this, only called on destroy.
  return !Object.keys(this._pending).length;
};

StatefulClient.prototype.emitMessage = function (name, params, cb) {
  if (this._destroyed) {
    asyncAvroError('client destroyed', cb);
    return;
  }

  var self = this;
  if (!this._started) {
    this.once('start', function () { self.emitMessage(name, params, cb); });
    return;
  }

  var tap = new Tap(new Buffer(this._bufferSize));
  var message = this._protocol.getMessage(name);
  if (!message) {
    asyncAvroError('unknown message', cb);
    return;
  }

  var id = this._id++;
  try {
    safeWrite(tap, this._idType, -id);
    this._encodeRequest(tap, message, params);
  } catch (err) {
    asyncAvroError(err, cb);
    return;
  }

  this._pending[id] = {message: message, cb: cb};
  this._encoder.write(tap.getValue());
};

StatefulClient.prototype.destroy = function () {
  this._destroyed = true;
  if (!this._started) {
    this.emit('start'); // Error out all pending calls.
  }
  if (this._noPending()) {
    this.emit('end');
  }
};

/**
 * Message receiver.
 *
 * A server can be listening to multiple clients at a given time. This can be a
 * mix of stateful or stateless clients (each will have its corresponding
 * channel, see below).
 *
 */
function Server(protocol) {
  events.EventEmitter.call(this);

  this._protocol = protocol;
  this._handlers = {};
  this._channels = [];
}
util.inherits(Server, events.EventEmitter);

Server.prototype.addTransport = function (transport, opts) {
  var channel;
  if (typeof transport == 'function') {
    // These channels are transient, so no need to keep a reference.
    channel = new StatelessChannel(this, transport, opts);
  } else {
    var readable, writable;
    if (transport instanceof stream.Duplex) {
      readable = writable = transport;
    } else {
      readable = transport.readable;
      writable = transport.writable;
    }
    channel = new StatefulChannel(this, readable, writable, opts);
    this._channels.push(channel);
  }

  var self = this;
  channel
    .on('request', function (name, req, cb) {
      var handler = self._handlers[name];
      if (!handler) {
        cb(new Error('not supported'));
      } else {
        handler(req, cb);
      }
    })
    .on('error', function (err) { self.emit('error', err); });

  return this;
};

Server.prototype.onMessage = function (name, opts, handler) {
  if (!handler && typeof opts == 'function') {
    handler = opts;
    opts = undefined;
  }

  if (!this._protocol._messages[name]) {
    throw new Error('unknown message');
  }

  if (this._handlers[name] && (!opts || !opts.override)) {
    throw new Error('already listening');
  }
  this._handlers[name] = handler;

  return this;
};

Server.prototype.destroy = function () {
  // TODO: Close all stateful channels.
};

/**
 * The server-side client equivalent.
 *
 * In particular it is responsible for handling handshakes appropriately.
 *
 */
function Channel(server, opts) {
  events.EventEmitter.call(this);
  opts = opts || {};

  this._server = server;
  this._clientHashString = null;

  var Type = opts.IdType || IdType;
  this._idType = new Type({type: 'map', values: 'bytes'});
  this._bufferSize = opts.bufferSize || 1024;
  this._protocolOpts = opts.protocolOpts;
}
util.inherits(Channel, events.EventEmitter);

Channel.prototype._decodeHandshake = function (tap) {
  var hs = HANDSHAKE_REQUEST_TYPE._read(tap);
  this._clientHashString = hs.clientHash.toString('binary');

  var match = hs.serverHash.toString('binary') === this._server._hashString;
  var serverPt = this._server._protocol;
  if (serverPt._canResolve(this._clientHashString)) {
    return match ? 'BOTH' : 'CLIENT';
  }

  if (!hs.clientProtocol) {
    return 'NONE';
  }

  var clientPt = new Protocol(hs.clientProtocol.string, this._protocolOpts);
  serverPt._addResolvers(clientPt);
  return match ? 'BOTH' : 'CLIENT';
};

Channel.prototype._encodeHandshake = function (tap, match) {
  var val;
  if (match === 'BOTH') {
    val = {match: 'BOTH', serverProtocol: null, serverHash: null, meta: null};
  } else {
    var lp = this._server._protocol;
    val = {
      match: match,
      serverProtocol: {string: lp.toString()},
      serverHash: {'org.apache.avro.ipc.MD5': lp.getHash()},
      meta: null
    };
  }
  safeWrite(tap, HANDSHAKE_RESPONSE_TYPE, val);
};

Channel.prototype._decodeRequest = function (tap, message) {
  var lp = this._server._protocol;
  var resolvers = lp._getResolvers(this._clientHashString, message);
  var val = resolvers._requestType._read(tap);
  if (!tap.isValid()) {
    throw new Error('truncated data');
  }
  if (tap.pos > tap.buf.length) {
    throw new Error('trailing data');
  }
  return val;
};

Channel.prototype._encodeResponse = function (tap, message, err, res) {
  var noError = err === null;
  safeWrite(tap, BOOLEAN_TYPE, !noError);
  if (noError) {
    safeWrite(tap, message._responseType, res);
  } else {
    safeWrite(tap, message._errorType, err);
  }
};

/**
 *
 *
 */
function StatelessChannel(server, readableFactory) {
  Channel.call(this, server);
  this._tap = new Tap(new Buffer(self._server._bufferSize));
  this._writable = undefined;
  this._message = undefined;
  this._match = undefined;
  this._id = undefined;

  var self = this;
  readableFactory(function (writable) {
    self._encoder = new MessageEncoder();
    self._encoder.pipe(writable);
    self.emit('_writable');
  }).pipe(new MessageDecoder())
    .once('data', onRequest);

  function onRequest(buf) {
    var tap = new Tap(buf);

    self._match = self._decodeHandshake(tap);
    if (self._match === 'NONE') {
      self._encodeHandshake(self._tap, 'NONE');
      return;
    }

    try {
      var name = STRING_TYPE._read(tap);
      self._message = self._server._protocol.getMessage(name);
      self._id = -self._idType._read(tap);
      var req = self._decodeRequest(tap, self._message);
    } catch (err) {
      self.emit('error', err);
    }

    self.emit('request', req, onResponse);
  }

  function onResponse(err, res) {
    try {
      self._encodeHandshake(self._tap, self._match);
      safeWrite(self._tap, self._idType, self._id);
      self._encodeResponse(self._tap, self._message, err, res);
    } catch (err) {
      self.emit('error', err);
      return;
    }

    if (self._encoder) {
      onWritable();
    } else {
      self.once('_writable', onWritable);
    }
  }

  function onWritable() { self._encoder.end(self._tap.getValue()); }
}
util.inherits(StatelessChannel, Channel);

/**
 *
 *
 */
function StatefulChannel(server, readable, writable) {
  Channel.call(this, server);
  this._readable = readable;
  this._writable = writable;
  this._decoder = new MessageDecoder();
  this._encoder = new MessageEncoder();

  this._readable
    .pipe(this._decoder)
    .on('data', onHandshake);

  this._encoder.pipe(this._writable);

  var self = this;

  function onHandshake(buf) {
    var tap = new Tap(new Buffer(self._bufferSize));

    var match = self._decodeHandshake(new Tap(buf));
    if (match !== 'NONE') {
      self._decoder
        .removeListener('data', onHandshake)
        .on('data', onRequest);
    }

    self._encodeHandshake(tap, match);
    self._encoder.write(tap.getValue());
  }

  function onRequest(buf) {
    var tap = new Tap(buf);

    try {
      var id = -self._idType._read(tap);
      var name = STRING_TYPE._read(tap);
      var message = self._server._protocol.getMessage(name);
      var req = self._decodeRequest(tap, message);
    } catch (err) {
      self.emit('error', err);
    }

    self.emit('request', name, req, function (err, res) {
      var tap = new Tap(new Buffer(self._bufferSize));
      try {
        safeWrite(tap, self._idType, id);
        self._encodeResponse(tap, message, err, res);
      } catch (err) {
        self.emit('error', err);
        return;
      }
      self._encoder.write(tap.getValue());
    });
  }
}
util.inherits(StatefulChannel, Channel);


// Helpers.

/**
 * An Avro message.
 *
 */
function Message(name, attrs, opts) {
  this._name = name;

  this._requestType = schemas.createType({
    name: name,
    type: 'message',
    fields: attrs.request
  }, opts);

  if (!attrs.response) {
    throw new Error('missing response');
  }
  this._responseType = schemas.createType(attrs.response, opts);

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

Message.prototype.getRequestType = function () { return this._requestType; };
Message.prototype.getResponseType = function () { return this._responseType; };
Message.prototype.getErrorType = function () { return this._errorType; };
Message.prototype.isOneWay = function () { return this.onWay; };
Message.prototype.toJSON = function () {
  var obj = {
    request: this._requestType.getFields(),
    response: this._responseType
  };
  var types = this._errorType.getTypes();
  if (types.length > 1) {
    obj.errors = schemas.createType(types.slice(1));
  }
  return obj;
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

IdType.prototype._fromValue = function (val) {
  var buf = val.id;
  return buf ? buf.readInt32BE() : 0;
};

IdType.prototype._toValue = function (any) {
  return {id: intBuffer(any | 0)};
};

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
function safeWrite(tap, type, val) {
  var pos = tap.pos;
  type._write(tap, val);

  if (!tap.isValid()) {
    var buf = new Buffer(tap.pos);
    tap.buf.copy(buf, 0, 0, pos);
    tap.buf = buf;
    tap.pos = pos;
    type._write(tap, val);
  }
}

/**
 * Async error handling.
 *
 * Also convert error into valid format for Avro.
 *
 */
function asyncAvroError(err, cb) {
  if (err instanceof Error) {
    err = err.message;
  }
  if (typeof err == 'string') {
    err = {string: err};
  }
  process.nextTick(function () { cb(err); });
}


module.exports = {
  Protocol: Protocol,
  channels: {
    StatefulChannel: StatefulChannel,
    StatelessChannel: StatelessChannel
  },
  clients: {
    StatefulClient: StatefulClient,
    StatelessClient: StatelessClient
  },
  streams: {
    MessageDecoder: MessageDecoder,
    MessageEncoder: MessageEncoder
  }
};
