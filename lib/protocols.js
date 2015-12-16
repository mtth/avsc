/* jshint node: true */

// TODO: Proxy all channel errors to both the server and the response.
// TODO: Find sensible behavior when destroying a server.
// TODO: Timeout for client options?
// TODO: handshake event rather than protocol hook. The latter isn't as useful
// (since parsing options aren't relevant when only creating resolvers).

'use strict';

var schemas = require('./schemas'),
    utils = require('./utils'),
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

  // Caching a string instead of the buffer to avoid retaining an entire slab.
  this._hashString = utils.getFingerprint(this.toString()).toString('binary');

  // Resolvers are split since we want clients to still be able to talk to
  // servers with more messages (which would be incompatible the other way).
  this._clientResolvers = {};
  this._serverResolvers = {};
}

Protocol.prototype.getName = function () { return this._name; };

Protocol.prototype.getHash = function () {
  return new Buffer(this._hashString, 'binary');
};

Protocol.prototype.getMessage = function (name) {
  return this._messages[name];
};

Protocol.prototype.createClient = function (transport, opts) {
  opts = opts || {};
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

Protocol.prototype.createServer = function () { return new Server(this); };

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
function Client(ptcl, opts) {
  opts = opts || {};
  events.EventEmitter.call(this);

  this._ptcl = ptcl;
  this._serverHashString = ptcl._hashString;
  this._idType = IdType.createMetadataType(opts.IdType);
  this._bufferSize = opts.bufferSize || 1024;
  this._frameSize = opts.frameSize || 8192;
  this._ptclHook = opts.protocolHook;
}
util.inherits(Client, events.EventEmitter);

Client.prototype._generateResolvers = function (hashString, serverPtcl) {
  if (hashString === this._ptcl._hashString) {
    return;
  }

  var resolvers = {};
  var clientMessages = this._ptcl._messages;
  var serverMessages = serverPtcl._messages;
  Object.keys(clientMessages || {}).forEach(function (name) {
    var cm = clientMessages[name];
    var sm = serverMessages[name];
    resolvers[name] = {
      responseType: cm.responseType.createResolver(sm.responseType),
      errorType: cm.errorType.createResolver(sm.errorType)
    };
  });
  this._ptcl._clientResolvers[hashString] = resolvers;
};

Client.prototype._canResolve = function (hashString) {
  var ptcl = this._ptcl;
  var clientResolvers = ptcl._clientResolvers;
  return hashString === ptcl._hashString || !!clientResolvers[hashString];
};

Client.prototype._getResolvers = function (hashString, message) {
  var ptcl = this._ptcl;
  if (hashString === ptcl._hashString) {
    return message;
  }
  var clientResolvers = ptcl._clientResolvers[hashString];
  return clientResolvers && clientResolvers[message.name];
};

Client.prototype._encodeHandshake = function (tap, includePtcl) {
  var ptcl = this._ptcl;
  var handshake = {
    clientHash: ptcl.getHash(),
    clientProtocol: includePtcl ? {string: ptcl.toString()} : null,
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

  var serverHash = handshake.serverHash['org.apache.avro.ipc.MD5'];
  var serverPtcl = parseProtocol(
    handshake.serverProtocol.string,
    serverHash,
    this._ptclHook
  );

  // Use the handshake's hash rather than our computed one in case they compute
  // it differently (e.g. the python client).
  this._serverHashString = serverHash.toString('binary');
  this._generateResolvers(this._serverHashString, serverPtcl);
  return handshake.match === 'CLIENT';
};

Client.prototype._encodeRequest = function (tap, message, req) {
  safeWrite(tap, STRING_TYPE, message.name);
  safeWrite(tap, message.requestType, req);
  return tap.getValue();
};

Client.prototype._decodeResponse = function (tap, message) {
  var resolvers = this._getResolvers(this._serverHashString, message);
  var args = [null, null];
  if (tap.readBoolean()) {
    args[0] = resolvers.errorType._read(tap);
  } else {
    args[1] = resolvers.responseType._read(tap);
  }
  if (!tap.isValid()) {
    throw new Error('invalid response');
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
 * advantage of this client is that it is able to keep track of which response
 * corresponds to each request without relying on messages' metadata. In
 * particular, this means these clients are compatible with any server
 * implementation.
 *
 */
function StatelessClient(ptcl, writableFactory, opts) {
  opts = opts || {};
  Client.call(this, ptcl, opts);

  this._writableFactory = writableFactory;
  this._pending = 0;
}
util.inherits(StatelessClient, Client);

StatelessClient.prototype.emitMessage = function (name, req, cb) {
  if (this._destroyed && !this._retrying) {
    asyncAvroError('client destroyed', cb);
    return;
  }

  var message = this._ptcl.getMessage(name);
  if (!message) {
    asyncAvroError(f('unknown message: %s', name), cb);
    return;
  }

  var self = this;
  (function emit(retry) {
    // Buffer the request's data.
    var tap = new Tap(new Buffer(self._bufferSize));
    try {
      self._encodeHandshake(tap, retry);
      safeWrite(tap, self._idType, 0); // Unused for stateless clients.
      self._encodeRequest(tap, message, req);
    } catch (err) {
      asyncAvroError(err, cb);
      return;
    }

    var writable = self._writableFactory(function (readable) {
      var decoder = new MessageDecoder();
      readable
        .pipe(decoder)
        .once('data', function (buf) {
          var tap = new Tap(buf);
          try {
            if (!self._decodeHandshake(tap)) {
              // Handshake failed, server doesn't have client protocol.
              emit(true);
              return;
            } else {
              self._idType._read(tap); // Skip metadata.
              cb.apply(undefined, self._decodeResponse(tap, message));
            }
          } catch (err) {
            asyncAvroError(err, cb);
          }
          if (!--self.pending && self._destroyed) {
            self.emit('eot');
          }
        });
    });

    var encoder = new MessageEncoder(self._frameSize);
    encoder.pipe(writable);
    encoder.end(tap.getValue());
    if (!retry) {
      self._pending++;
    }
  })(false);
};

StatelessClient.prototype.destroy = function () {
  this._destroyed = true;
  if (!this._pending) {
    this.emit('eot');
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
function StatefulClient(ptcl, readable, writable, opts) {
  opts = opts || {};
  Client.call(this, ptcl, opts);

  this._readable = readable;
  this._writable = writable;
  this._started = false;
  this._destroyed = false;
  this._id = 1;
  this._pending = {};

  var self = this;

  this._decoder = this._readable
    .pipe(new MessageDecoder())
    .on('data', onHandshakeData)
    .on('end', function () {
      if (!self._noPending()) {
        self.emit('error', new Error('interrupted'));
      }
      self.destroy();
    });

  this._encoder = new MessageEncoder(this._frameSize);
  this._encoder.pipe(this._writable);
  emitHandshake(false);

  this.on('eot', function () {
    this._readable.unpipe(this._decoder);
    this._encoder.unpipe(this._writable);
  });

  function emitHandshake(includePtcl) {
    var tap = new Tap(new Buffer(self._bufferSize));
    self._encodeHandshake(tap, includePtcl);
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
      self.emit('_start'); // Send any pending messages.
    } else {
      emitHandshake(true);
    }
  }

  function onMessageData(buf) {
    var tap = new Tap(buf);
    try {
      var id = self._idType._read(tap);
    } catch (err) {
      self.emit('error', err);
      return;
    }

    var info = self._pending[id];
    if (info === undefined) {
      self.emit('error', new Error('orphan response'));
      return;
    }

    delete self._pending[id];
    if (self._destroyed && self._noPending()) {
      self.emit('eot');
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

StatefulClient.prototype.emitMessage = function (name, req, cb) {
  if (this._destroyed) {
    asyncAvroError('client destroyed', cb);
    return;
  }

  var self = this;
  if (!this._started) {
    this.once('_start', function () { self.emitMessage(name, req, cb); });
    return;
  }

  var tap = new Tap(new Buffer(this._bufferSize));
  var message = this._ptcl.getMessage(name);
  if (!message) {
    asyncAvroError('unknown message', cb);
    return;
  }

  var id = this._id++;
  try {
    safeWrite(tap, this._idType, -id);
    this._encodeRequest(tap, message, req);
  } catch (err) {
    asyncAvroError(err, cb);
    return;
  }

  if (!message.oneWay) {
    this._pending[id] = {message: message, cb: cb};
  }
  this._encoder.write(tap.getValue());
};

StatefulClient.prototype.destroy = function () {
  this._destroyed = true;
  if (!this._started) {
    this.emit('_start'); // Error out any pending calls.
  }
  if (this._noPending()) {
    this.emit('eot');
  }
};

/**
 * Message receiver.
 *
 * A server can be listening to multiple clients at a given time. This can be a
 * mix of stateful or stateless clients (each will have its corresponding
 * channel, see below).
 *
 * For performance reasons (and simplicity), a server doesn't directly keep
 * track of its attached channels. The downside of this is that channels can't
 * be destroyed until they next receive a request.
 *
 */
function Server(ptcl) {
  events.EventEmitter.call(this);

  this._ptcl = ptcl;
  this._handlers = {};

  var self = this;
  this._onChannelCall = function (name, req, cb) {
    var handler = self._handlers[name];
    if (!handler) {
      cb(new Error(f('unsupported message: %s', name)));
    } else {
      handler(req, cb);
    }
  };
}
util.inherits(Server, events.EventEmitter);

Server.prototype.createChannel = function (transport, opts) {
  var channel;
  if (typeof transport == 'function') {
    channel = new StatelessChannel(this._ptcl, transport, opts);
  } else {
    var readable, writable;
    if (transport instanceof stream.Duplex) {
      readable = writable = transport;
    } else {
      readable = transport.readable;
      writable = transport.writable;
    }
    channel = new StatefulChannel(this._ptcl, readable, writable, opts);
  }
  return channel.on('_call', this._onChannelCall);
};

Server.prototype.onMessage = function (name, opts, handler) {
  if (!handler && typeof opts == 'function') {
    handler = opts;
    opts = undefined;
  }

  if (!this._ptcl._messages[name]) {
    throw new Error(f('unknown message: %s', name));
  }
  if (this._handlers[name] && (!opts || !opts.override)) {
    throw new Error(('already listening to %s', name));
  }
  this._handlers[name] = handler;

  return this;
};

/**
 * The server-side client equivalent.
 *
 * In particular it is responsible for handling handshakes appropriately.
 *
 */
function Channel(ptcl, opts) {
  events.EventEmitter.call(this);
  opts = opts || {};

  this._ptcl = ptcl;
  this._clientHashString = null;
  this._idType = IdType.createMetadataType(opts.IdType);
  this._bufferSize = opts.bufferSize || 1024;
  this._frameSize = opts.frameSize || 8192;
  this._ptclHook = opts.protocolHook;
}
util.inherits(Channel, events.EventEmitter);

Channel.prototype._generateResolvers = function (hashString, clientPtcl) {
  if (hashString === this._ptcl._hashString) {
    return;
  }

  var resolvers = {};
  var clientMessages = clientPtcl._messages;
  var serverMessages = this._ptcl._messages;
  Object.keys(clientMessages || {}).forEach(function (name) {
    var sm = serverMessages[name];
    if (!sm) {
      throw new Error(f('missing server message: %s', name));
    }
    var cm = clientMessages[name];
    if (!resolvers[name]) {
      resolvers[name] = {};
    }
    var resolver = sm.requestType.createResolver(cm.requestType);
    resolvers[name].requestType = resolver;
  });
  this._ptcl._serverResolvers[hashString] = resolvers;
};

Channel.prototype._canResolve = function (hashString) {
  var ptcl = this._ptcl;
  var serverResolvers = ptcl._serverResolvers;
  return hashString === ptcl._hashString || !!serverResolvers[hashString];
};

Channel.prototype._getResolvers = function (hashString, message) {
  var ptcl = this._ptcl;
  if (hashString === ptcl._hashString) {
    return message;
  }
  var serverResolvers = ptcl._serverResolvers[hashString];
  return serverResolvers && serverResolvers[message.name];
};

Channel.prototype._decodeHandshake = function (tap) {
  var handshake = HANDSHAKE_REQUEST_TYPE._read(tap);
  var clientHash = handshake.clientHash;
  this._clientHashString = clientHash.toString('binary');

  var match = handshake.serverHash.equals(this._ptcl.getHash());
  if (this._canResolve(this._clientHashString)) {
    return match ? 'BOTH' : 'CLIENT';
  }

  if (!handshake.clientProtocol) {
    return 'NONE';
  }

  var clientPtcl = parseProtocol(
    handshake.clientProtocol.string,
    clientHash,
    this._ptclHook
  );

  this._generateResolvers(this._clientHashString, clientPtcl);
  return match ? 'BOTH' : 'CLIENT';
};

Channel.prototype._encodeHandshake = function (tap, match) {
  var val;
  if (match === 'BOTH') {
    val = {match: 'BOTH', serverProtocol: null, serverHash: null, meta: null};
  } else {
    val = {
      match: match,
      serverProtocol: {string: this._ptcl.toString()},
      serverHash: {'org.apache.avro.ipc.MD5': this._ptcl.getHash()},
      meta: null
    };
  }
  safeWrite(tap, HANDSHAKE_RESPONSE_TYPE, val);
};

Channel.prototype._decodeRequest = function (tap, message) {
  var resolvers = this._getResolvers(this._clientHashString, message);
  var val = resolvers.requestType._read(tap);
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
    safeWrite(tap, message.responseType, res);
  } else {
    if (err instanceof Error) {
      // Convenience to allow client to use JS errors inside handlers.
      err = {string: err.message};
    }
    safeWrite(tap, message.errorType, err);
  }
};

/**
 * Channel for stateless transport.
 *
 * This channel expect a handshake to precede each message.
 *
 */
function StatelessChannel(ptcl, readableFactory, opts) {
  Channel.call(this, ptcl);

  this._tap = new Tap(new Buffer(opts && opts.bufferSize || 1024));
  this._writable = undefined;
  this._message = undefined;
  this._match = undefined;
  this._id = undefined;
  this._decoder = new MessageEncoder();
  this._encoder = new MessageEncoder(this._frameSize);

  var self = this;
  this._readable = readableFactory(function (writable) {
    self._encoder
      .pipe(writable)
      .on('finish', function () { self.emit('eot'); });
    self.emit('_writable');
  }).pipe(this._decoder)
    .once('data', onRequestData);

  function onRequestData(buf) {
    var tap = new Tap(buf);

    self._match = self._decodeHandshake(tap);
    if (self._match === 'NONE') {
      self._encodeHandshake(self._tap, 'NONE');
      return;
    }

    try {
      var name = STRING_TYPE._read(tap);
      self._message = self._ptcl.getMessage(name);
      self._id = -self._idType._read(tap);
      var req = self._decodeRequest(tap, self._message);
    } catch (err) {
      self.emit('error', err);
    }

    self._readable.unpipe(self._decoder);
    self.emit('_call', req, onResponse);
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

StatelessChannel.prototype.destroy = function () {}; // Nothing to do here.

/**
 * Stateful transport channel.
 *
 * A handshake is done when the channel is first opened, then all messages are
 * sent without.
 *
 */
function StatefulChannel(ptcl, readable, writable) {
  Channel.call(this, ptcl);

  this._readable = readable;
  this._writable = writable;
  this._decoder = new MessageDecoder();
  this._encoder = new MessageEncoder(this._frameSize);
  this._pending = 0;

  var self = this;

  this._readable
    .pipe(this._decoder)
    .on('data', onHandshakeData);

  this._encoder
    .pipe(this._writable);

  this.on('eot', function () {
    this._readable.unpipe(this._decoder);
    this._encoder.unpipe(this._writable);
  });

  function onHandshakeData(buf) {
    var match = self._decodeHandshake(new Tap(buf));
    if (match !== 'NONE') {
      self._decoder
        .removeListener('data', onHandshakeData)
        .on('data', onRequestData);
    }

    var tap = new Tap(new Buffer(self._bufferSize));
    self._encodeHandshake(tap, match);
    self._encoder.write(tap.getValue());
  }

  function onRequestData(buf) {
    var tap = new Tap(buf);
    try {
      var id = -self._idType._read(tap);
    } catch (err) {
      // We need the ID to be able to respond. We can't do anything better
      // here.
      self.emit('error', err);
      return;
    }

    if (self._destroyed) {
      onResponse({string: 'channel destroyed'});
      return;
    }

    try {
      var name = STRING_TYPE._read(tap);
      var message = self._ptcl.getMessage(name);
      var req = self._decodeRequest(tap, message);
    } catch (err) {
      onResponse(err);
      return;
    }

    self._pending++;
    self.emit('_call', name, req, message.oneWay ? undefined : onResponse);

    function onResponse(err, res) {
      var tap = new Tap(new Buffer(self._bufferSize));
      try {
        safeWrite(tap, self._idType, id);
        self._encodeResponse(tap, message, err, res);
      } catch (err) {
        self.emit('error', err);
        done();
        return;
      }
      self._encoder.write(tap.getValue(), undefined, done);

      function done() {
        if (!--self._pending && self._destroyed) {
          self.emit('eot');
        }
      }
    }
  }

}
util.inherits(StatefulChannel, Channel);

StatefulChannel.prototype.destroy = function () {
  this._destroyed = true;
  if (!this._pending) {
    this.emit('eot');
  }
};

// Helpers.

/**
 * An Avro message.
 *
 */
function Message(name, attrs, opts) {
  this.name = name;

  this.requestType = schemas.createType({
    name: 'Message',
    type: 'message',
    fields: attrs.request
  }, opts);

  if (!attrs.response) {
    throw new Error('missing response');
  }
  this.responseType = schemas.createType(attrs.response, opts);

  var errors = attrs.errors || [];
  errors.unshift('string');
  this.errorType = schemas.createType(errors, opts);

  this.oneWay = !!attrs['one-way'];
  if (this.oneWay) {
    if (
      !(this.responseType instanceof schemas.types.NullType) ||
      errors.length > 1
    ) {
      throw new Error('unapplicable one-way parameter');
    }
  }
}

Message.prototype.toJSON = function () {
  var obj = {
    request: this.requestType.getFields(),
    response: this.responseType
  };
  var types = this.errorType.getTypes();
  if (types.length > 1) {
    obj.errors = schemas.createType(types.slice(1));
  }
  return obj;
};

/**
 * "Framing" stream.
 *
 */
function MessageEncoder(frameSize) {
  stream.Transform.call(this);
  this._frameSize = frameSize;
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
 * "Un-framing" stream.
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
 * This is required for stateful clients to work and can be overridden to read
 * or write arbitrary metadata. Note that the message contents are
 * (intentionally) not available when updating this metadata.
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

IdType.createMetadataType = function (Type) {
  Type = Type || IdType;
  return new Type({type: 'map', values: 'bytes'});
};

/**
 * Returns a buffer containing an integer's big-endian representation.
 *
 * @param n {Number} Integer.
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
 * @param tap {Tap} Tap written to.
 * @param type {Type} Avro type.
 * @param val {...} Corresponding Avro value.
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
 * Asynchronous error handling.
 *
 * @param err {...} Error. If an `Error` instance or a string, it will be
 * converted into valid format for Avro.
 * @param cb {Function} Callback to which the error will be passed as first and
 * single argument.
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

/**
 * Parse remote protocol, with hook intercept.
 *
 */
function parseProtocol(ptclString, hash, hook) {
  var attrs = JSON.parse(ptclString);
  var ptcl;
  if (hook) {
    ptcl = hook(attrs, hash);
    if (ptcl && !(ptcl instanceof Protocol)) {
      throw new Error('invalid protocol hook');
    }
  }
  if (!ptcl) {
    ptcl = new Protocol(attrs);
  }
  return ptcl;
}


module.exports = {
  Message: Message,
  Protocol: Protocol,
  Server: Server,
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
