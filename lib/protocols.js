/* jshint node: true */

// TODO: Explore making `MessageEmitter` a writable stream, and
// `MessageListener` a readable stream. The main inconsistency is w.r.t.
// watermarks (the standard stream behavior doesn't support waiting for the
// callbacks, without also preventing concurrent requests).
// TODO: See whether it is worth it to remove listeners from readable and
// writable streams when stateless emitters and listeners are destroyed.
// TODO: Add protocol "discover" method?

'use strict';

/**
 * This module implements Avro's IPC/RPC logic.
 *
 * This is done the Node.js way, mimicking the `EventEmitter` class.
 *
 */

var types = require('./types'),
    utils = require('./utils'),
    events = require('events'),
    stream = require('stream'),
    util = require('util');


// Various useful types. We instantiate options once, to share the registry.

var OPTS = {};

var BOOLEAN_TYPE = types.createType('boolean', OPTS);
var MAP_BYTES_TYPE = types.createType({type: 'map', values: 'bytes'}, OPTS);
var STRING_TYPE = types.createType('string', OPTS);

var HANDSHAKE_REQUEST_TYPE = types.createType({
  name: 'org.apache.avro.ipc.HandshakeRequest',
  type: 'record',
  fields: [
    {name: 'clientHash', type: {name: 'MD5', type: 'fixed', size: 16}},
    {name: 'clientProtocol', type: ['null', 'string'], 'default': null},
    {name: 'serverHash', type: 'MD5'},
    {name: 'meta', type: ['null', MAP_BYTES_TYPE], 'default': null}
  ]
}, OPTS);

var HANDSHAKE_RESPONSE_TYPE = types.createType({
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
    {name: 'serverProtocol', type: ['null', 'string'], 'default': null},
    {name: 'serverHash', type: ['null', 'MD5'], 'default': null},
    {name: 'meta', type: ['null', MAP_BYTES_TYPE], 'default': null}
  ]
}, OPTS);

// A few convenience imports.

var Tap = utils.Tap;
var f = util.format;

/**
 * Protocol generation function.
 *
 * This should be used instead of the protocol constructor. The protocol's
 * constructor performs no logic to better support efficient protocol copy.
 *
 */
function createProtocol(attrs, opts) {
  opts = opts || {};

  var name = attrs.protocol;
  if (!name) {
    throw new Error('missing protocol name');
  }
  if (attrs.namespace !== undefined) {
    opts.namespace = attrs.namespace;
  } else {
    var match = /^(.*)\.[^.]+$/.exec(name);
    if (match) {
      opts.namespace = match[1];
    }
  }
  name = types.qualify(name, opts.namespace);
  if (attrs.types) {
    attrs.types.forEach(function (obj) { types.createType(obj, opts); });
  }
  var messages = {};
  if (attrs.messages) {
    Object.keys(attrs.messages).forEach(function (key) {
      messages[key] = new Message(key, attrs.messages[key], opts);
    });
  }
  return new Protocol(name, messages, opts.registry || {});
}

/**
 * An Avro protocol.
 *
 */
function Protocol(name, messages, types, handlers) {
  if (types === undefined) {
    // Let's be helpful in case this class is instantiated directly.
    return createProtocol(name, messages);
  }

  this._name = name;
  this._messages = messages;
  this._types = types;
  // Shared with subprotocols (via the prototype chain, overwriting is safe).
  this._handlers = handlers || {};
  // We cache a string rather than a buffer to not retain an entire slab. This
  // also lets us more use hashes as keys inside maps (e.g. for resolvers).
  this._hs = utils.getHash(this.getSchema()).toString('binary');
}

Protocol.prototype.subprotocol = function () {
  // Return a copy of the protocol, but a separate namespace for handlers which
  // inherits from the parent protocol. This can be useful for organizing
  // protocols when there are many handlers.
  return new Protocol(
    this._name,
    this._messages,
    this._types,
    Object.create(this._handlers)
  );
};

Protocol.prototype.createEmitter = function (transport, opts) {
  var objectMode = opts && opts.objectMode;
  if (typeof transport == 'function') {
    var writableFactory;
    if (objectMode) {
      writableFactory = transport;
    } else {
      // We provide a default standard-compliant codec. This should support
      // most use-cases (for example when speaking to the official Java and
      // Python implementations over HTTP, or when this library is used for
      // both the emitting and listening sides).
      writableFactory = function (cb) {
        var encoder = new FrameEncoder(opts);
        encoder.pipe(transport(function (err, readable) {
          if (err) {
            cb(err);
            return;
          }
          cb(null, readable.pipe(new FrameDecoder()));
        }));
        return encoder;
      };
    }
    return new StatelessEmitter(this, writableFactory, opts);
  } else {
    var readable, writable;
    if (isStream(transport)) {
      readable = writable = transport;
    } else {
      readable = transport.readable;
      writable = transport.writable;
    }
    if (!objectMode) {
      // To ease communication with Java servers, we provide a non-standard
      // default codec here (but compatible with Java servers'
      // `NettyTransportCodec`'s implementation). This is unfortunate but
      // probably a good compromise in practice.
      readable = readable.pipe(new NettyDecoder());
      var encoder = new NettyEncoder();
      encoder.pipe(writable);
      writable = encoder;
    }
    return new StatefulEmitter(this, readable, writable, opts);
  }
};

Protocol.prototype.createListener = function (transport, opts) {
  // See `createEmitter` for `objectMode` motivations.
  var objectMode = opts && opts.objectMode;
  if (typeof transport == 'function') {
    var readableFactory;
    if (objectMode) {
      readableFactory = transport;
    } else {
      readableFactory = function (cb) {
        return transport(function (err, writable) {
          if (err) {
            cb(err);
            return;
          }
          var encoder = new FrameEncoder(opts);
          encoder.pipe(writable);
          cb(null, encoder);
        }).pipe(new FrameDecoder());
      };
    }
    return new StatelessListener(this, readableFactory, opts);
  } else {
    var readable, writable;
    if (isStream(transport)) {
      readable = writable = transport;
    } else {
      readable = transport.readable;
      writable = transport.writable;
    }
    if (!objectMode) {
      readable = readable.pipe(new NettyDecoder());
      var encoder = new NettyEncoder();
      encoder.pipe(writable);
      writable = encoder;
    }
    return new StatefulListener(this, readable, writable, opts);
  }
};

Protocol.prototype.emit = function (name, req, emitter, cb) {
  if (!emitter || !this.equals(emitter.getProtocol())) {
    throw new Error('invalid emitter');
  }
  var message = this._messages[name];
  if (!message) {
    throw new Error('unknown message: ' + name);
  }
  var self = this;
  emitter.emitMessage(name, {request: req}, function (err, resEnv) {
    var errType = message.getErrorType();
    // System error, likely the message wasn't sent (or an error occurred while
    // decoding the response).
    if (err) {
      if (this._strict) {
        err = errType.clone(err.message, {wrapUnions: true});
      }
      done(err);
      return;
    }
    // Message transmission succeeded, we transmit the message data; massaging
    // any error strings into actual `Error` objects in non-strict mode.
    err = resEnv.error;
    if (!this._strict) {
      if (err === undefined) {
        err = null;
      } else if (types.Type.isType(errType, 'union:unwrapped')) {
        if (typeof err == 'string') {
          err = new Error(err);
        }
      } else if (err && err.string) {
        err = new Error(err.string);
      }
    }
    done(err, resEnv.response);
  });
  return emitter.getPending();

  function done(err, res) {
    if (cb) {
      cb.call(self, err, res);
    } else if (err) {
      emitter.emit('error', err);
    }
  }
};

Protocol.prototype.on = function (name, handler) {
  if (!this._messages[name]) {
    throw new Error(f('unknown message: %s', name));
  }
  this._handlers[name] = handler;
  return this;
};

Protocol.prototype.getHandler = function (name) {
  return this._handlers[name];
};

Protocol.prototype.getName = function () { return this._name; };

Protocol.prototype.getType = function (name) { return this._types[name]; };

Protocol.prototype.getMessage = function (name) {
  return this._messages[name];
};

Protocol.prototype.getMessages = function () {
  var messages = this._messages;
  return Object.keys(messages).map(function (name) { return messages[name]; });
};

Protocol.prototype.getSchema = function (opts) {
  var namedTypes = [];
  Object.keys(this._types).forEach(function (name) {
    var type = this._types[name];
    if (type.getName()) { // Skip primitives.
      namedTypes.push(type);
    }
  }, this);
  return types.stringify({
    protocol: this._name,
    types: namedTypes.length ? namedTypes : undefined,
    messages: Object.keys(this._messages).length ? this._messages : undefined
  }, opts);
};

Protocol.prototype.getFingerprint = function (algorithm) {
  if (!algorithm) {
    // We can use the cached hash.
    return new Buffer(this._hs, 'binary');
  } else {
    return utils.getHash(this.getSchema());
  }
};

Protocol.prototype.equals = function (ptcl) {
  return !!ptcl && this._hs === ptcl._hs;
};

Protocol.prototype.toString = function () {
  return this.getSchema({noDeref: true});
};

Protocol.prototype.inspect = function () {
  return f('<Protocol %j>', this._name);
};

Protocol.MessageEmitter = MessageEmitter;
Protocol.MessageListener = MessageListener;

/**
 * Base message emitter class.
 *
 * See below for the two available variants.
 *
 */
function MessageEmitter(ptcl, opts) {
  opts = opts || {};
  events.EventEmitter.call(this);

  this._ptcl = ptcl;
  this._strict = !!opts.strictErrors;
  this._endWritable = !!utils.getOption(opts, 'endWritable', true);
  this._timeout = utils.getOption(opts, 'timeout', 10000);

  this._cache = opts.cache || {};
  var fgpt = opts.serverFingerprint;
  var adapter;
  if (fgpt) {
    adapter = this._cache[fgpt];
  }
  if (!adapter) {
    // This might happen even if the server fingerprint option was set, in
    // cases where the cache doesn't contain the corresponding adapter.
    fgpt = ptcl.getFingerprint();
    adapter = this._cache[fgpt] = new Adapter(ptcl, ptcl, fgpt);
  }
  this._adapter = adapter;

  this._registry = new Registry(this);
  this._destroyed = false;
  this._interrupted = false;
  this.once('_eot', function (pending) { this.emit('eot', pending); });
}
util.inherits(MessageEmitter, events.EventEmitter);

MessageEmitter.prototype.getCache = function () { return this._cache; };

MessageEmitter.prototype.getProtocol = function () { return this._ptcl; };

MessageEmitter.prototype.getTimeout = function () { return this._timeout; };

MessageEmitter.prototype.isDestroyed = function () { return this._destroyed; };

MessageEmitter.prototype.getPending = function () {
  return this._registry.size();
};

MessageEmitter.prototype.emitMessage = function (name, reqEnv, opts, cb) {
  if (cb === undefined && typeof opts == 'function') {
    cb = opts;
    opts = undefined;
  }
  if (!cb) {
    throw new Error('missing callback');
  }

  // Serialize the message.
  var err, msg, reqBuf;
  if (this._destroyed) {
    err = new Error('destroyed');
  } else if (name === '') {
    // This is a ping request.
    reqBuf = new Buffer([0, 0]); // No header, empty message name.
  } else {
    msg = this._ptcl.getMessage(name);
    if (!msg) {
      err = new Error('missing message');
    } else {
      try {
        reqBuf = Buffer.concat([
          MAP_BYTES_TYPE.toBuffer(reqEnv.header || {}),
          STRING_TYPE.toBuffer(name),
          msg.getRequestType().toBuffer(reqEnv.request)
        ]);
      } catch (cause) {
        err = wrapError('invalid request', cause);
      }
    }
  }

  // Return now if a serialization error occurred.
  var self = this;
  if (err) {
    process.nextTick(function () { cb.call(self, err); });
    return true;
  }

  // Generate the response callback.
  var timeout = (opts && opts.timeout !== undefined) ?
    opts.timeout :
    this._timeout;
  var id = this._registry.add(timeout, function (err, resBuf, adapter) {
    var resEnv;
    if (!err) {
      if (name === '') {
        resEnv = {};
      } else {
        try {
          resEnv = adapter.decodeResponse(resBuf, name).envelope;
        } catch (cause) {
          err = wrapError('invalid response', cause);
        }
      }
    }
    var meta;
    if (adapter) {
      meta = {
        serverFingerprint: adapter._fingerprint,
        serverProtocol: adapter.getServerProtocol()
      };
    }
    cb.call(this, err, resEnv, meta);
    if (this._destroyed && !this._interrupted && !this._registry.size()) {
      this.destroy();
    }
  });

  return this._send(id, reqBuf, !!msg && msg.isOneWay());
};

MessageEmitter.prototype.destroy = function (noWait) {
  this._destroyed = true;
  var registry = this._registry;
  var pending = registry.size();
  if (noWait && pending) {
    this._interrupted = true;
    registry.clear();
  }
  if (noWait || !pending) {
    this.emit('_eot', pending);
  }
};

MessageEmitter.prototype._send = utils.abstractFunction;

MessageEmitter.prototype._createHandshakeRequest = function (adapter, noPtcl) {
  var ptcl = this._ptcl;
  return {
    clientHash: ptcl.getFingerprint(),
    clientProtocol: noPtcl ? null : ptcl.getSchema({exportAttrs: true}),
    serverHash: adapter._fingerprint
  };
};

MessageEmitter.prototype._getAdapter = function (hres) {
  var serverBuf = hres.serverHash;
  var adapter = this._cache[serverBuf];
  if (adapter) {
    return adapter;
  }
  var serverPtcl = createProtocol(JSON.parse(hres.serverProtocol));
  adapter = new Adapter(this._ptcl, serverPtcl, serverBuf);
  return this._cache[serverBuf] = adapter;
};

/**
 * Factory-based emitter.
 *
 * This emitter doesn't keep a persistent connection to the server and requires
 * prepending a handshake to each message emitted. Usage examples include
 * talking to an HTTP server (where the factory returns an HTTP request).
 *
 * Since each message will use its own writable/readable stream pair, the
 * advantage of this emitter is that it is able to keep track of which response
 * corresponds to each request without relying on transport ordering. In
 * particular, this means these emitters are compatible with any server
 * implementation.
 *
 */
function StatelessEmitter(ptcl, writableFactory, opts) {
  MessageEmitter.call(this, ptcl, opts);
  this._writableFactory = writableFactory;

  if (!opts || !opts.noPing) {
    // Ping the server to check whether the remote protocol is compatible.
    this.emitMessage('', {}, function (err) {
      if (err) {
        this.emit('error', err);
      }
    });
  }
}
util.inherits(StatelessEmitter, MessageEmitter);

StatelessEmitter.prototype._send = function (id, reqBuf) {
  var cb = this._registry.get(id);
  var adapter = this._adapter;
  var self = this;
  process.nextTick(emit);
  return true; // Each writable is only used once, no risk of buffering.

  function emit(retry) {
    var hreq = self._createHandshakeRequest(adapter, !retry);

    var writable = self._writableFactory.call(self, function (err, readable) {
      if (err) {
        cb(err);
        return;
      }
      readable.on('data', function (obj) {
        var buf = Buffer.concat(obj.payload);
        try {
          var parts = readHead(HANDSHAKE_RESPONSE_TYPE, buf);
          var hres = parts.head;
          if (hres.serverHash) {
            adapter = self._getAdapter(hres);
          }
          self.emit('handshake', hreq, hres);
          if (hres.match === 'NONE') {
            emit(true);
            return;
          }
          // Change the default adapter.
          self._adapter = adapter;
        } catch (err) {
          cb(err);
          return;
        }
        cb(null, parts.tail, adapter);
      });
    });

    writable.write({
      id: id,
      payload: [HANDSHAKE_REQUEST_TYPE.toBuffer(hreq), reqBuf]
    });
    if (self._endWritable) {
      writable.end();
    }
  }
};

/**
 * Multiplexing emitter.
 *
 * These emitters reuse the same streams (both readable and writable) for all
 * messages. This avoids a lot of overhead (e.g. creating new connections,
 * re-issuing handshakes) but requires the underlying transport to support
 * forwarding message IDs.
 *
 */
function StatefulEmitter(ptcl, readable, writable, opts) {
  MessageEmitter.call(this, ptcl, opts);
  this._readable = readable;
  this._writable = writable;
  this._connected = !!(opts && opts.noPing);
  this._readable.on('end', function () { self.destroy(true); });
  this._writable.on('finish', function () { self.destroy(); });

  this.on('eot', function () {
    // Remove references to this emitter to avoid potential memory leaks.
    if (this._endWritable) {
      this._writable.end();
    }
    this._readable
      .removeListener('data', onPing)
      .removeListener('data', onMessage);
  });

  var self = this;
  var hreq; // For handshake events.
  if (this._connected) {
    this._readable.on('data', onMessage);
  } else {
    this._readable.on('data', onPing);
    process.nextTick(ping);
  }

  function ping(retry) {
    if (self._destroyed) {
      return;
    }
    hreq = self._createHandshakeRequest(self._adapter, !retry);
    var payload = [
      HANDSHAKE_REQUEST_TYPE.toBuffer(hreq),
      new Buffer([0, 0]) // No header, no data (empty message name).
    ];
    self._writable.write({id: 0, payload: payload});
  }

  function onPing(obj) {
    var buf = Buffer.concat(obj.payload);
    try {
      var hres = readHead(HANDSHAKE_RESPONSE_TYPE, buf).head;
      if (hres.serverHash) {
        self._adapter = self._getAdapter(hres);
      }
    } catch (err) {
      self.destroy(true); // Not a recoverable error.
      self.emit('error', wrapError('handshake error', err));
      return;
    }
    self.emit('handshake', hreq, hres);
    if (hres.match === 'NONE') {
      ping(true);
    } else {
      self._readable.removeListener('data', onPing).on('data', onMessage);
      self._connected = true;
      self.emit('_connected');
      hreq = null; // Release reference.
    }
  }

  // Callback used after a connection has been established.
  function onMessage(obj) {
    var cb = self._registry.get(obj.id);
    if (cb) {
      process.nextTick(function () {
        // Ensure that the initial callback gets called asynchronously, even
        // for completely synchronous transports (otherwise the number of
        // pending requests will sometimes be inconsistent between stateful and
        // stateless transports).
        cb(null, Buffer.concat(obj.payload), self._adapter);
      });
    }
  }
}
util.inherits(StatefulEmitter, MessageEmitter);

StatefulEmitter.prototype._send = function (id, reqBuf, isOneWay) {
  if (!this._connected) {
    this.once('_connected', function () { this._send(id, reqBuf, isOneWay); });
    return false; // Call is being buffered.
  }
  if (isOneWay) {
    var self = this;
    // Clear the callback, passing in an empty header.
    process.nextTick(function () {
      self._registry.get(id)(null, new Buffer([0, 0, 0]), self._adapter);
    });
  }
  return this._writable.write({id: id, payload: [reqBuf]});
};

/**
 * The server-side emitter equivalent.
 *
 */
function MessageListener(ptcl, opts) {
  opts = opts || {};
  events.EventEmitter.call(this);

  this._ptcl = ptcl;
  this._strict = !!opts.strictErrors;
  this._endWritable = !!utils.getOption(opts, 'endWritable', true);

  this._cache = opts.cache || {};
  var fgpt = this._ptcl.getFingerprint();
  if (!this._cache[fgpt]) {
    // Add the listener's protocol to the cache if it isn't already there. This
    // will save a handshake the first time on emitters with the same protocol.
    this._cache[fgpt] = new Adapter(this._ptcl, this._ptcl, fgpt);
  }

  this._adapter = null;
  this._hook = null;

  this._pending = 0;
  this._destroyed = false;
  this._interrupted = false;
  this.once('_eot', function (pending) { this.emit('eot', pending); });
}
util.inherits(MessageListener, events.EventEmitter);

MessageListener.prototype.getCache = function () { return this._cache; };

MessageListener.prototype.getPending = function () { return this._pending; };

MessageListener.prototype.getProtocol = function () { return this._ptcl; };

MessageListener.prototype.isDestroyed = function () {
  return this._destroyed;
};

MessageListener.prototype.onMessage = function (fn) {
  this._hook = fn;
  return this;
};

MessageListener.prototype.destroy = function (noWait) {
  this._destroyed = true;
  if (noWait || !this._pending) {
    this._interrupted = true;
    this.emit('_eot', this._pending);
  }
};

MessageListener.prototype._receive = function (reqBuf, adapter, cb) {
  var ptcl = this._ptcl;
  var self = this;
  try {
    var decoded = adapter.decodeRequest(reqBuf);
  } catch (err) {
    cb(encodeError(err));
    return;
  }

  var clientMsg = decoded.message;
  if (!clientMsg) {
    // Ping request, return an empty response.
    cb(new Buffer(0));
    return;
  }

  var name = clientMsg.getName();
  var serverMsg = ptcl.getMessage(name);
  var handler = ptcl._handlers[name];
  var reqEnv = decoded.envelope;
  var isDone = false;
  this._pending++;
  if (this._hook) {
    // Custom hook.
    var meta = {
      clientFingerprint: adapter._fingerprint,
      clientProtocol: adapter.getClientProtocol()
    };
    this._hook.call(this, name, reqEnv, meta, done);
  } else if (handler) {
    if (serverMsg.isOneWay()) {
      handler.call(ptcl, reqEnv.request);
      done(null, {error: undefined, response: null});
    } else {
      try {
        handler.call(ptcl, reqEnv.request, this, function (err, res) {
          done(null, {error: err, response: res});
        });
      } catch (err) {
        // We catch synchronous failures (same as express) and return the
        // failure. Note that the server process can still crash if an error is
        // thrown after the handler returns but before the response is sent
        // (again, same as express). We don't do this for one-way messages
        // because potential errors would then easily pass unnoticed.
        done(err);
      }
    }
  } else {
    // The underlying protocol hasn't implemented a handler for this message.
    done(new Error(f('unhandled message: %s', name)));
  }

  function done(err, resEnv) {
    if (isDone) {
      self.emit('error', new Error('message callback called multiple times'));
      return;
    }
    isDone = true;
    self._pending--;
    var resBuf;
    if (!err) {
      var errType = serverMsg.getErrorType();
      var resErr = resEnv.error;
      if (!self._strict) {
        if (isError(resErr)) {
          resErr = errType.clone(resErr.message, {wrapUnions: true});
        } else if (resErr === null) {
          resErr = undefined;
        }
      }
      var noError = resErr === undefined;
      try {
        var header = MAP_BYTES_TYPE.toBuffer(resEnv.header || {});
        resBuf = Buffer.concat([
          header,
          BOOLEAN_TYPE.toBuffer(!noError),
          noError ?
            serverMsg.getResponseType().toBuffer(resEnv.response) :
            errType.toBuffer(resErr)
        ]);
      } catch (cause) {
        err = wrapError('invalid response', cause);
      }
    }
    if (err) {
      resBuf = encodeError(err, header);
    }
    if (!self._interrupted) {
      cb(resBuf, serverMsg.isOneWay());
    }
    if (self._destroyed && !self._pending) {
      self.destroy();
    }
  }
};

MessageListener.prototype._createHandshakeResponse = function (err, hreq) {
  var ptcl = this._ptcl;
  var buf = ptcl.getFingerprint();
  var serverMatch = hreq && hreq.serverHash.equals(buf);
  return {
    match: err ? 'NONE' : (serverMatch ? 'BOTH' : 'CLIENT'),
    serverProtocol: serverMatch ? null : ptcl.getSchema({exportAttrs: true}),
    serverHash: serverMatch ? null : buf
  };
};

MessageListener.prototype._getAdapter = function (hreq) {
  var clientBuf = hreq.clientHash;
  var adapter = this._cache[clientBuf];
  if (adapter) {
    return adapter;
  }
  if (!hreq.clientProtocol) {
    throw new Error('unknown protocol');
  }
  var clientPtcl = createProtocol(JSON.parse(hreq.clientProtocol));
  adapter = new Adapter(clientPtcl, this._ptcl, clientBuf);
  return this._cache[clientBuf] = adapter;
};

/**
 * MessageListener for stateless transport.
 *
 * This listener expect a handshake to precede each message.
 *
 */
function StatelessListener(ptcl, readableFactory, opts) {
  MessageListener.call(this, ptcl, opts);
  var self = this;
  var readable;

  process.nextTick(function () {
    // Delay listening to allow handlers to be attached even if the factory is
    // purely synchronous.
    readable = readableFactory.call(this, function (err, writable) {
      if (err) {
        self.emit('error', err);
        // Since stateless listeners are only used once, it is safe to destroy.
        onFinish();
        return;
      }
      self._writable = writable.on('finish', onFinish);
      self.emit('_writable');
    }).on('data', onRequest)
      .on('end', onEnd);
  });

  function onRequest(obj) {
    var id = obj.id;
    var buf = Buffer.concat(obj.payload);
    var err = null;
    try {
      var parts = readHead(HANDSHAKE_REQUEST_TYPE, buf);
      var hreq = parts.head;
      var adapter = self._getAdapter(hreq);
    } catch (cause) {
      err = wrapError('invalid handshake request', cause);
    }

    if (err) {
      done(encodeError(err));
    } else {
      self._receive(parts.tail, adapter, done);
    }

    function done(resBuf) {
      if (!self._writable) {
        self.once('_writable', function () { done(resBuf); });
        return;
      }
      var hres = self._createHandshakeResponse(err, hreq);
      self.emit('handshake', hreq, hres);
      var payload = [
        HANDSHAKE_RESPONSE_TYPE.toBuffer(hres),
        resBuf
      ];
      self._writable.write({id: id, payload: payload});
      if (self._endWritable) {
        self._writable.end();
      }
    }
  }

  function onEnd() { self.destroy(); }

  function onFinish() {
    if (readable) {
      readable
        .removeListener('data', onRequest)
        .removeListener('end', onEnd);
    }
    self.destroy(true);
  }
}
util.inherits(StatelessListener, MessageListener);

/**
 * Stateful transport listener.
 *
 * A handshake is done when the listener is first opened, then all messages are
 * sent without.
 *
 */
function StatefulListener(ptcl, readable, writable, opts) {
  MessageListener.call(this, ptcl, opts);
  this._adapter = undefined;
  this._writable = writable.on('finish', onFinish);
  this._readable = readable.on('data', onHandshake).on('end', onEnd);

  this.on('eot', function () {
    if (this._endWritable) {
      this._writable.end();
    }
    // Also clean up any references to the listener on the underlying streams.
    this._writable.removeListener('finish', onFinish);
    this._readable
      .removeListener('data', onHandshake)
      .removeListener('data', onRequest)
      .removeListener('end', onEnd);
  });

  var self = this;

  function onHandshake(obj) {
    var id = obj.id;
    var buf = Buffer.concat(obj.payload);
    var err;
    try {
      var parts = readHead(HANDSHAKE_REQUEST_TYPE, buf);
      var hreq = parts.head;
      self._adapter = self._getAdapter(hreq);
    } catch (cause) {
      err = wrapError('invalid handshake request', cause);
    }
    if (err) {
      // Either the client's protocol was unknown or it isn't compatible.
      done(encodeError(err));
    } else {
      self._readable
        .removeListener('data', onHandshake)
        .on('data', onRequest);
      self._receive(parts.tail, self._adapter, done);
    }

    function done(resBuf) {
      var hres = self._createHandshakeResponse(err, hreq);
      self.emit('handshake', hreq, hres);
      var payload = [
        HANDSHAKE_RESPONSE_TYPE.toBuffer(hres),
        resBuf
      ];
      self._writable.write({id: id, payload: payload});
    }
  }

  function onRequest(obj) {
    // These requests are not prefixed with handshakes.
    var id = obj.id;
    var reqBuf = Buffer.concat(obj.payload);
    self._receive(reqBuf, self._adapter, function (resBuf, isOneWay) {
      if (!isOneWay) {
        self._writable.write({id: id, payload: [resBuf]});
      }
    });
  }

  function onEnd() { self.destroy(); }

  function onFinish() { self.destroy(true); }
}
util.inherits(StatefulListener, MessageListener);

/**
 * An Avro message.
 *
 * It contains the various types used to send it (request, error, response).
 *
 */
function Message(name, attrs, opts) {
  opts = opts || {};

  if (!types.isValidName(name)) {
    throw new Error(f('invalid message name: %s', name));
  }
  this._name = name;

  var recordName = f('org.apache.avro.ipc.%sRequest', name);
  this._requestType = types.createType({
    name: recordName,
    type: 'record',
    namespace: opts.namespace || '', // Don't leak request namespace.
    fields: attrs.request
  }, opts);
  // We remove the record from the registry to prevent it from being exported
  // in the protocol's schema.
  delete opts.registry[recordName];

  if (!attrs.response) {
    throw new Error('missing response');
  }
  this._responseType = types.createType(attrs.response, opts);

  var errors = attrs.errors || [];
  errors.unshift('string');
  this._errorType = types.createType(errors, opts);

  this._oneWay = !!attrs['one-way'];
  if (this._oneWay) {
    if (this._responseType.getTypeName() !== 'null' || errors.length > 1) {
      throw new Error('unapplicable one-way parameter');
    }
  }
}

Message.prototype.getName = function () { return this._name; };

Message.prototype.getRequestType = function () { return this._requestType; };

Message.prototype.getResponseType = function () { return this._responseType; };

Message.prototype.getErrorType = function () { return this._errorType; };

Message.prototype.isOneWay = function () { return this._oneWay; };

Message.prototype.inspect = Message.prototype.toJSON = function () {
  var obj = {
    request: this._requestType.getFields(),
    response: this._responseType
  };
  var errorTypes = this._errorType.getTypes();
  if (errorTypes.length > 1) {
    obj.errors = types.createType(errorTypes.slice(1));
  }
  if (this._oneWay) {
    obj['one-way'] = true;
  }
  return obj;
};

// Helpers.

/**
 * Callback registry.
 *
 * Callbacks added must accept an error as first argument. This is used by
 * message emitters to store pending calls.
 *
 */
function Registry(ctx) {
  this._ctx = ctx; // Context for all callbacks.
  this._id = 0; // Unique integer ID for each call.
  this._n = 0; // Number of pending calls.
  this._cbs = {};
}

Registry.prototype.size = function () { return this._n; };

Registry.prototype.get = function (id) { return this._cbs[id]; };

Registry.prototype.add = function (timeout, fn) {
  this._id = (this._id + 1) | 0;

  var self = this;
  var id = this._id;
  var timer;
  if (timeout > 0) {
    timer = setTimeout(function () { cb(new Error('timeout')); }, timeout);
  }

  this._cbs[id] = cb;
  this._n++;
  return id;

  function cb() {
    if (!self._cbs[id]) {
      // The callback has already run.
      return;
    }
    delete self._cbs[id];
    self._n--;
    if (timer) {
      clearTimeout(timer);
    }
    fn.apply(self._ctx, arguments);
  }
};

Registry.prototype.clear = function () {
  Object.keys(this._cbs).forEach(function (id) {
    this._cbs[id](new Error('interrupted'));
  }, this);
};

/**
 * Protocol resolution helper.
 *
 * It is used both by emitters and listeners, to respectively decode errors and
 * responses, or requests.
 *
 */
function Adapter(clientPtcl, serverPtcl, fingerprint) {
  this._clientPtcl = clientPtcl;
  this._serverPtcl = serverPtcl;
  this._fingerprint = fingerprint; // Convenience.
  this._rsvs = clientPtcl.equals(serverPtcl) ? null : this._createResolvers();
}

Adapter.prototype.getClientProtocol = function () { return this._clientPtcl; };

Adapter.prototype.getServerProtocol = function () { return this._serverPtcl; };

Adapter.prototype._createResolvers = function () {
  var rsvs = {};
  this._clientPtcl.getMessages().forEach(function (c) {
    var n = c.getName();
    var s = this._serverPtcl.getMessage(n);
    if (!s) {
      throw new Error(f('missing server message: %s', n));
    }
    if (s.isOneWay() !== c.isOneWay()) {
      throw new Error(f('inconsistent one-way parameter for message: %s', n));
    }
    try {
      rsvs[n + '?'] = s.getRequestType().createResolver(c.getRequestType());
      rsvs[n + '*'] = c.getErrorType().createResolver(s.getErrorType());
      rsvs[n + '!'] = c.getResponseType().createResolver(s.getResponseType());
    } catch (err) {
      throw wrapError('incompatible message ' + n, err);
    }
  }, this);
  return rsvs;
};

Adapter.prototype._getReader = function (name, qualifier) {
  if (this._rsvs) {
    return this._rsvs[name + qualifier];
  } else {
    var msg = this._serverPtcl.getMessage(name);
    switch (qualifier) {
      case '?': return msg.getRequestType();
      case '*': return msg.getErrorType();
      case '!': return msg.getResponseType();
    }
  }
};

Adapter.prototype.decodeRequest = function (buf) {
  var tap = new Tap(buf);
  var hdr = MAP_BYTES_TYPE._read(tap);
  var name = STRING_TYPE._read(tap);
  if (name) {
    var req = this._getReader(name, '?')._read(tap);
  }
  if (!tap.isValid()) {
    throw new Error('truncated request');
  }
  return {
    message: this._clientPtcl.getMessage(name),
    envelope: {header: hdr, request: req}
  };
};

Adapter.prototype.decodeResponse = function (buf, name) {
  var tap = new Tap(buf);
  var hdr = MAP_BYTES_TYPE._read(tap);
  var isError = BOOLEAN_TYPE._read(tap);
  var reader = this._getReader(name, isError ? '*' : '!');
  if (isError) {
    var err = reader._read(tap);
  } else {
    var res = reader._read(tap);
  }
  if (!tap.isValid()) {
    throw new Error('truncated response');
  }
  return {
    message: this._serverPtcl.getMessage(name),
    envelope: {header: hdr, error: err, response: res}
  };
};

/**
 * Standard "un-framing" stream.
 *
 */
function FrameDecoder() {
  stream.Transform.call(this, {readableObjectMode: true});
  this._id = undefined;
  this._buf = new Buffer(0);
  this._bufs = [];

  this.on('finish', function () { this.push(null); });
}
util.inherits(FrameDecoder, stream.Transform);

FrameDecoder.prototype._transform = function (buf, encoding, cb) {
  buf = Buffer.concat([this._buf, buf]);
  var frameLength;
  while (
    buf.length >= 4 &&
    buf.length >= (frameLength = buf.readInt32BE(0)) + 4
  ) {
    if (frameLength) {
      this._bufs.push(buf.slice(4, frameLength + 4));
    } else {
      var bufs = this._bufs;
      this._bufs = [];
      this.push({id: null, payload: bufs});
    }
    buf = buf.slice(frameLength + 4);
  }
  this._buf = buf;
  cb();
};

FrameDecoder.prototype._flush = function () {
  if (this._buf.length || this._bufs.length) {
    this.emit('error', new Error('trailing data'));
  }
};

/**
 * Standard framing stream.
 *
 * @param `frameSize` {Number} (Maximum) size in bytes of each frame. The last
 * frame might be shorter. Defaults to 4096.
 *
 */
function FrameEncoder() {
  stream.Transform.call(this, {writableObjectMode: true});
  this.on('finish', function () { this.push(null); });
}
util.inherits(FrameEncoder, stream.Transform);

FrameEncoder.prototype._transform = function (obj, encoding, cb) {
  var bufs = obj.payload;
  var i, l, buf;
  for (i = 0, l = bufs.length; i < l; i++) {
    buf = bufs[i];
    this.push(intBuffer(buf.length));
    this.push(buf);
  }
  this.push(intBuffer(0));
  cb();
};

/**
 * Netty-compatible decoding stream.
 *
 */
function NettyDecoder() {
  stream.Transform.call(this, {readableObjectMode: true});
  this._id = undefined;
  this._frameCount = 0;
  this._buf = new Buffer(0);
  this._bufs = [];

  this.on('finish', function () { this.push(null); });
}
util.inherits(NettyDecoder, stream.Transform);

NettyDecoder.prototype._transform = function (buf, encoding, cb) {
  buf = Buffer.concat([this._buf, buf]);

  while (true) {
    if (this._id === undefined) {
      if (buf.length < 8) {
        this._buf = buf;
        cb();
        return;
      }
      this._id = buf.readInt32BE(0);
      this._frameCount = buf.readInt32BE(4);
      buf = buf.slice(8);
    }

    var frameLength;
    while (
      this._frameCount &&
      buf.length >= 4 &&
      buf.length >= (frameLength = buf.readInt32BE(0)) + 4
    ) {
      this._frameCount--;
      this._bufs.push(buf.slice(4, frameLength + 4));
      buf = buf.slice(frameLength + 4);
    }

    if (this._frameCount) {
      this._buf = buf;
      cb();
      return;
    } else {
      var obj = {id: this._id, payload: this._bufs};
      this._bufs = [];
      this._id = undefined;
      this.push(obj);
    }
  }
};

NettyDecoder.prototype._flush = function () {
  if (this._buf.length || this._bufs.length) {
    this.emit('error', new Error('trailing data'));
  }
};

/**
 * Netty-compatible encoding stream.
 *
 */
function NettyEncoder() {
  stream.Transform.call(this, {writableObjectMode: true});
  this.on('finish', function () { this.push(null); });
}
util.inherits(NettyEncoder, stream.Transform);

NettyEncoder.prototype._transform = function (obj, encoding, cb) {
  var bufs = obj.payload;
  var l = bufs.length;
  var buf;
  // Header: [ ID, number of frames ]
  buf = new Buffer(8);
  buf.writeInt32BE(obj.id, 0);
  buf.writeInt32BE(l, 4);
  this.push(buf);
  // Frames, each: [ length, bytes ]
  var i;
  for (i = 0; i < l; i++) {
    buf = bufs[i];
    this.push(intBuffer(buf.length));
    this.push(buf);
  }
  cb();
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
 * Decode a type used as prefix inside a buffer.
 *
 * @param type {Type} The type of the prefix.
 * @param buf {Buffer} Encoded bytes.
 *
 * This function will return an object `{head, tail}` where head contains the
 * decoded value and tail the rest of the buffer. An error will be thrown if
 * the prefix cannot be decoded.
 *
 */
function readHead(type, buf) {
  var tap = new Tap(buf);
  var head = type._read(tap);
  if (!tap.isValid()) {
    throw new Error(f('truncated %s', type));
  }
  return {head: head, tail: tap.buf.slice(tap.pos)};
}

/**
 * Wrap something in an error.
 *
 * @param message {String} The new error's message.
 * @param cause {Error} The cause of the error. It is available as `cause`
 * field on the outer error.
 *
 * This is used to keep the argument of emitters' `'error'` event errors.
 *
 */
function wrapError(message, cause) {
  var err = new Error(f('%s: %s', message, cause.message));
  err.cause = cause;
  return err;
}

/**
 * Check whether something is an error.
 *
 * @param any {Object} Any object.
 *
 */
function isError(any) {
  // Also not ideal, but avoids brittle `instanceof` checks.
  return !!any && Object.prototype.toString.call(any) === '[object Error]';
}

/**
 * Encode an error and optional header into a valid Avro response.
 *
 * @param err {Error} Error to encode.
 * @param header {Object} Optional response header.
 *
 */
function encodeError(err, header) {
  return Buffer.concat([
    header || new Buffer([0]), // Recover the header if possible.
    new Buffer([1, 0]), // Error flag and first union index.
    STRING_TYPE.toBuffer(err.message)
  ]);
}

/**
 * Check whether something is a stream.
 *
 * @param any {Object} Any object.
 *
 */
function isStream(any) {
  // This is a hacky way of checking that the transport is a stream-like
  // object. We unfortunately can't use `instanceof Stream` checks since
  // some libraries (e.g. websocket-stream) return streams which don't
  // inherit from it.
  return !!any.pipe;
}


module.exports = {
  HANDSHAKE_REQUEST_TYPE: HANDSHAKE_REQUEST_TYPE,
  HANDSHAKE_RESPONSE_TYPE: HANDSHAKE_RESPONSE_TYPE,
  Message: Message,
  Protocol: Protocol,
  Registry: Registry,
  createProtocol: createProtocol,
  streams: {
    FrameDecoder: FrameDecoder,
    FrameEncoder: FrameEncoder,
    NettyDecoder: NettyDecoder,
    NettyEncoder: NettyEncoder
  }
};
