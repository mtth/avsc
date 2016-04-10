/* jshint node: true */

// TODO: Simplify decoders and encoders to use the fact that payloads are a
// single buffer rather than a list.
// TODO: Explre making `Emitter` a writable stream, and `Listener` a readable
// stream. The main inconsistency is w.r.t. watermarks (the standard stream
// behavior doesn't support waiting for the callbacks, without also preventing
// concurrent requests).
// TODO: Remove allowing special characters in message names.
// TODO: Put all message decoding/resolution logic in the protocol class;
// emitters and resolvers are now only in charge of handshakes and headers.
// This should help simplify logic de-duplication.
// TODO: Replace de/encoders with simpler versions using only a single frame.
// This will reduce the size of this module and remove the inaccessible
// frame-size option, and simplify interface with emitters/resolvers (which can
// now pass a buffer rather than an array of buffers).
// TODO: Allow stateless stream functions to return undefined to signal an
// error (i.e. handle that politely).
// TODO: Return status of last transport write when emitting (rather than
// pending count which can be obtained using a `getPendingCount` call).
// TODO: Optimize EnvelopeEncoder by avoiding the extra copy on transform.
// TODO: Add protocol `discover` method?
// TODO: Add clear protocol cache method?

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
  namespace: 'org.apache.avro.ipc',
  name: 'HandshakeRequest',
  type: 'record',
  fields: [
    {name: 'clientHash', type: {name: 'MD5', type: 'fixed', size: 16}},
    {name: 'clientProtocol', type: ['null', 'string'], 'default': null},
    {name: 'serverHash', type: 'MD5'},
    {name: 'meta', type: ['null', MAP_BYTES_TYPE], 'default': null}
  ]
}, OPTS);

var HANDSHAKE_RESPONSE_TYPE = types.createType({
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
    {name: 'serverProtocol', type: ['null', 'string'], 'default': null},
    {name: 'serverHash', type: ['null', 'MD5'], 'default': null},
    {name: 'meta', type: ['null', MAP_BYTES_TYPE], 'default': null}
  ]
}, OPTS);

// A few convenience imports.

var Tap = utils.Tap;
var Type = types.Type;
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
  opts.namespace = attrs.namespace;
  if (opts.namespace && !~name.indexOf('.')) {
    name = f('%s.%s', opts.namespace, name);
  }
  if (attrs.types) {
    attrs.types.forEach(function (obj) { types.createType(obj, opts); });
  }
  var messages = {};
  if (attrs.messages) {
    Object.keys(attrs.messages).forEach(function (key) {
      messages[key] = new Message(key, attrs.messages[key], opts);
    });
  }
  return new Protocol(name, messages, opts.registry, opts.strictErrors);
}

/**
 * An Avro protocol.
 *
 */
function Protocol(name, messages, types, strict, handlers) {
  this._name = name;
  this._messages = messages || {};
  this._types = types || {};
  this._strict = !!strict;
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
    this._strict,
    Object.create(this._handlers)
  );
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
  var reqEnv = {message: message, request: req};
  emitter.emitMessage(reqEnv, undefined, function (err, resEnv) {
    var errType = message.getErrorType();
    // System error, likely the message wasn't sent (or an error occurred while
    // decoding the response).
    if (err) {
      if (self._strict) {
        err = errType.clone(err.message, {wrapUnions: true});
      }
      done(err);
      return;
    }
    // Message transmission succeeded, we transmit the message data; massaging
    // any error strings into actual `Error` objects in non-strict mode.
    err = resEnv.error;
    if (!self._strict) {
      if (err === undefined) {
        err = null;
      } else if (Type.isType(errType, 'union:unwrapped')) {
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

Protocol.prototype.createEmitter = function (transport, opts) {
  var objectMode = opts && opts.objectMode;
  var ee;
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
        encoder.pipe(transport(function (readable) {
          cb(readable.pipe(new FrameDecoder()));
        }));
        return encoder;
      };
    }
    ee = new StatelessEmitter(this, writableFactory, opts);
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
    ee = new StatefulEmitter(this, readable, writable);
  }
  return ee;
};

Protocol.prototype.createListener = function (transport, opts) {
  // See `createEmitter` for `objectMode` motivations.
  var objectMode = opts && opts.objectMode;
  var ee;
  if (typeof transport == 'function') {
    var readableFactory;
    if (objectMode) {
      readableFactory = transport;
    } else {
      readableFactory = function (cb) {
        return transport(function (writable) {
          var encoder = new FrameEncoder(opts);
          encoder.pipe(writable);
          cb(encoder);
        }).pipe(new FrameDecoder());
      };
    }
    ee = new StatelessListener(this, readableFactory, opts);
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
    ee = new StatefulListener(this, readable, writable, opts);
  }
  return ee;
};

Protocol.prototype.equals = function (ptcl) {
  return !!ptcl && this._hs === ptcl._hs;
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
    if (type.getName()) {
      namedTypes.push(type);
    }
  }, this);
  return types.stringify({
    protocol: this._name,
    types: namedTypes.length ? namedTypes : undefined,
    messages: this._messages
  }, opts);
};

Protocol.prototype.getFingerprint = function (algorithm) {
  if (algorithm === undefined) {
    // We can use the cached hash.
    return new Buffer(this._hs, 'binary');
  } else {
    return utils.getHash(this.getSchema());
  }
};

Protocol.prototype.toString = function () {
  return this.getSchema({noDeref: true});
};

Protocol.prototype.inspect = function () {
  return f('<Protocol %j>', this._name);
};


/**
 * Base message emitter class.
 *
 * See below for the two available variants.
 *
 */
function Emitter(ptcl, opts) {
  opts = opts || {};
  events.EventEmitter.call(this);
  this._ptcl = ptcl;
  this._timeout = opts.timeout === undefined ? 2000 : opts.timeout;
  this._cache = opts.cache || {};

  var serverPtcl = opts.serverProtocol || ptcl;
  this._decoder = this._cache[serverPtcl._hs] || new Decoder(serverPtcl);
  this._registry = new Registry(this);

  this._destroyed = false;
  this._interrupted = false;
  this.once('_eot', function (pending) { this.emit('eot', pending); });
}
util.inherits(Emitter, events.EventEmitter);

Emitter.prototype.getPending = function () { return this._registry.size(); };

Emitter.prototype.getProtocol = function () { return this._ptcl; };

Emitter.prototype.getTimeout = function () { return this._timeout; };

Emitter.prototype.emitMessage = function (reqEnv, timeout, cb) {
  if (!cb) {
    throw new Error('missing callback');
  }

  // Serialize the message.
  var err;
  if (this._destroyed) {
    err = new Error('destroyed');
  } else {
    var msg = reqEnv.message;
    if (!msg) {
      err = new Error('missing message');
    } else {
      try {
        var reqBuf = Buffer.concat([
          MAP_BYTES_TYPE.toBuffer(reqEnv.header || {}),
          STRING_TYPE.toBuffer(msg.getName()),
          msg.getRequestType().toBuffer(reqEnv.request)
        ]);
      } catch (cause) {
        err = wrapError('invalid request', cause);
      }
    }
  }
  if (err) {
    process.nextTick(function () { cb.call(this, err); });
    return;
  }

  // Generate the response callback.
  if (timeout === undefined) {
    timeout = this._timeout;
  }
  var id = this._registry.add(timeout, function (err, resBuf, decoder) {
    if (!err) {
      try {
        var resEnv = decoder.decodeResponse(resBuf, msg.getName());
      } catch (cause) {
        err = wrapError('invalid response', cause);
      }
    }
    cb.call(this, err, resEnv);
    if (this._destroyed && !this._interrupted && !this._registry.size()) {
      this.destroy();
    }
  });

  var self = this;
  process.nextTick(function () { self._send(id, reqBuf, msg.isOneWay()); });
};

Emitter.prototype.destroy = function (noWait) {
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

Emitter.prototype._send = utils.abstractFunction;

Emitter.prototype._createHandshakeRequest = function (decoder, includePtcl) {
  var ptcl = decoder.getClientProtocol();
  return {
    clientHash: ptcl.getFingerprint(),
    clientProtocol: includePtcl ? ptcl.getSchema({exportAttrs: true}) : null,
    serverHash: decoder.getServerProtocol().getFingerprint()
  };
};

Emitter.prototype._getDecoder = function (hres) {
  var clientBuf = this._ptcl.getFingerprint();
  var serverBuf = hres ? hres.serverHash : clientBuf;
  var decoder;
  if (serverBuf.equals(clientBuf)) {
    decoder = new Decoder(this._ptcl);
  } else {
    decoder = this._cache[serverBuf];
    if (decoder) {
      return decoder;
    }
    var serverPtcl = createProtocol(JSON.parse(hres.serverProtocol));
    decoder = new Decoder(this._ptcl, serverPtcl);
  }
  return this._cache[serverBuf] = decoder;
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
 * corresponds to each request without relying on messages' metadata. In
 * particular, this means these emitters are compatible with any server
 * implementation.
 *
 */
function StatelessEmitter(ptcl, writableFactory, opts) {
  Emitter.call(this, ptcl, opts);
  this._writableFactory = writableFactory;
}
util.inherits(StatelessEmitter, Emitter);

StatelessEmitter.prototype._send = function (id, reqBuf) {
  var decoder = this._decoder;
  var self = this;
  process.nextTick(emit);

  function emit(retry) {
    var hreq = self._createHandshakeRequest(decoder, retry);

    var writable = self._writableFactory(function (readable) {
      readable.on('data', function (obj) {
        var cb = self._registry.get(id);
        if (!cb) {
          // Likely timeout.
          return;
        }
        try {
          var parts = readHead(HANDSHAKE_RESPONSE_TYPE, obj.payload);
          var hres = parts.head;
          if (hres.serverHash) {
            decoder = self._getDecoder(hres);
          }
          self.emit('handshake', hreq, hres);
          if (hres.match === 'NONE') {
            emit(true);
            return;
          }
          // Change the default decoder.
          self._decoder = decoder;
        } catch (err) {
          debugger;
          cb(err);
          return;
        }
        cb(null, parts.tail, decoder);
      });
    });

    writable.end({
      id: id,
      payload: Buffer.concat([HANDSHAKE_REQUEST_TYPE.toBuffer(hreq), reqBuf])
    });
  }
};

/**
 * Multiplexing emitter.
 *
 * These emitters reuse the same streams (both readable and writable) for all
 * messages. This avoids a lot of overhead (e.g. creating new connections,
 * re-issuing handshakes) but requires the server to include compatible
 * metadata in each response (namely forwarding each request's ID into its
 * response).
 *
 * A custom metadata format can be specified via the `idType` option. The
 * default is compatible with this package's default server (i.e. listener)
 * implementation.
 *
 */
function StatefulEmitter(ptcl, readable, writable, opts) {
  Emitter.call(this, ptcl, opts);
  this._readable = readable;
  this._writable = writable;
  this._connected = false;

  this._readable
    .on('data', onPing)
    .on('end', function () { self.destroy(); });

  this._writable
    .on('finish', function () { self.destroy(); });

  var self = this;
  var hreq; // For the handshake event.
  process.nextTick(function () { ping(false); });

  function ping(retry) {
    if (self._destroyed) {
      return;
    }
    hreq = self._createHandshakeRequest(self._decoder, retry);
    var payload = Buffer.concat([
      HANDSHAKE_REQUEST_TYPE.toBuffer(hreq),
      new Buffer([0, 0]) // No header, no data (empty message name).
    ]);
    self._writable.write({id: 0, payload: payload});
  }

  function onPing(obj) {
    try {
      var hres = readHead(HANDSHAKE_RESPONSE_TYPE, obj.payload).head;
      if (hres.serverHash) {
        self._decoder = self._getDecoder(hres);
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
      cb(null, obj.payload, self._decoder);
    }
  }
}
util.inherits(StatefulEmitter, Emitter);

StatefulEmitter.prototype._send = function (id, reqBuf, isOneWay) {
  if (!this._connected) {
    this.once('_connected', function () { this._send(id, reqBuf, isOneWay); });
    return;
  }
  this._writable.write({id: id, payload: reqBuf});
  if (isOneWay) {
    this._registry.get(id)(null, new Buffer([0, 0, 0]), this._decoder);
  }
};

/**
 * The server-side emitter equivalent.
 *
 */
function Listener(ptcl, opts) {
  opts = opts || {};
  events.EventEmitter.call(this);
  this._ptcl = ptcl;
  this._cache = opts.cache || {};

  this._decoder = null;

  this._pending = 0;
  this._destroyed = false;
  this.once('_eot', function (pending) { this.emit('eot', pending); });
}
util.inherits(Listener, events.EventEmitter);

Listener.prototype.onMessage = function (fn) { this._hook = fn; };

Listener.prototype.destroy = function (noWait) {
  this._destroyed = true;
  if (noWait || !this._pending) {
    this.emit('_eot', this._pending);
  }
};

Listener.prototype._receive = function (reqBuf, decoder, cb) {
  var ptcl = this._ptcl;
  var self = this;

  try {
    var reqEnv = decoder.decodeRequest(reqBuf);
  } catch (err) {
    done(err);
    return;
  }
  var clientMsg = reqEnv.message;
  if (!clientMsg) {
    // Ping request.
    cb(new Buffer(0));
    return;
  }
  var name = clientMsg.getName();
  var serverMsg = ptcl.getMessage(name);
  var handler = ptcl._handlers[name];
  this._pending++;
  if (this._hook) {
    // Custom hook.
    this._hook.call(this, reqEnv, handler, done);
  } else if (handler) {
    try {
      if (serverMsg.isOneWay()) {
        handler.call(ptcl, reqEnv.request);
        done(null, null);
      } else {
        handler.call(ptcl, reqEnv.request, this, function (err, res) {
          var errType = serverMsg.getErrorType();
          if (!ptcl._strict) {
            if (isError(err)) {
              err = errType.clone(err.message, {wrapUnions: true});
            } else if (err === null) {
              err = undefined;
            }
          }
          done(null, {error: err, response: res});
        });
      }
    } catch (err) {
      // We catch synchronous failures (same as express) and return the
      // failure. Note that the server process can still crash if an error is
      // thrown after the handler returns but before the response is sent
      // (again, same as express).
      done(err);
    }
  } else {
    // The underlying protocol hasn't implemented a handler for this message.
    done(new Error(f('unhandled message: %s', name)));
  }

  function done(err, resEnv) {
    self._pending--;
    if (!err) {
      var errType = serverMsg.getErrorType();
      var noError = resEnv.error === undefined;
      try {
        var header = MAP_BYTES_TYPE.toBuffer(resEnv.header || {});
        var bufs = [
          header,
          BOOLEAN_TYPE.toBuffer(!noError),
          noError ?
            serverMsg.getResponseType().toBuffer(resEnv.response) :
            errType.toBuffer(resEnv.error)
        ];
      } catch (cause) {
        err = wrapError('invalid response', cause);
      }
    }
    if (err) {
      bufs = [
        header || new Buffer([0]), // Recover the header if possible.
        new Buffer([1, 0]), // Error flag and first union index.
        STRING_TYPE.toBuffer(err.message)
      ];
    }
    cb(Buffer.concat(bufs), serverMsg.isOneWay());
    if (self._destroyed && !self._pending) {
      self.destroy();
    }
  }
};

Listener.prototype._createHandshakeResponse = function (err, hreq) {
  var ptcl = this._ptcl;
  var buf = ptcl.getFingerprint();
  var serverMatch = hreq && hreq.serverHash.equals(buf);
  return {
    match: err ? 'NONE' : (serverMatch ? 'BOTH' : 'CLIENT'),
    serverProtocol: serverMatch ? null : ptcl.getSchema({exportAttrs: true}),
    serverHash: serverMatch ? null : buf
  };
};

Listener.prototype._getDecoder = function (hreq) {
  var serverBuf = this._ptcl.getFingerprint();
  var clientBuf = hreq ? hreq.clientHash : serverBuf;
  var decoder;
  if (clientBuf.equals(serverBuf)) {
    decoder = new Decoder(this._ptcl);
  } else {
    decoder = this._cache[clientBuf];
    if (decoder) {
      return decoder;
    }
    if (!hreq.clientProtocol) {
      throw new Error('unknown protocol');
    }
    var clientPtcl = createProtocol(JSON.parse(hreq.clientProtocol));
    decoder = new Decoder(clientPtcl, this._ptcl);
  }
  return this._cache[clientBuf] = decoder;
};

/**
 * Listener for stateless transport.
 *
 * This listener expect a handshake to precede each message.
 *
 */
function StatelessListener(ptcl, readableFactory, opts) {
  Listener.call(this, ptcl, opts);

  var self = this;

  readableFactory(function (writable) {
    self._writable = writable;
    self.emit('_writable');
  }).on('data', onRequest)
    .on('end', onEnd);

  function onRequest(obj) {
    var id = obj.id;
    var err = null;
    try {
      var parts = readHead(HANDSHAKE_REQUEST_TYPE, obj.payload);
      var hreq = parts.head;
      var decoder = self._getDecoder(hreq);
    } catch (cause) {
      err = wrapError('invalid handshake request', cause);
    }

    if (err) {
      done(Buffer.concat([
        new Buffer([0, 1, 0]),
        STRING_TYPE.toBuffer(err.message)
      ]));
    } else {
      self._receive(parts.tail, decoder, done);
    }

    function done(resBuf) {
      var hres = self._createHandshakeResponse(err, hreq);
      self.emit('handshake', hreq, hres);
      if (self._writable) {
        var payload = Buffer.concat([
          HANDSHAKE_RESPONSE_TYPE.toBuffer(hres),
          resBuf
        ]);
        self._writable.end({id: id, payload: payload});
      } else {
        self.once('_writable', function () { done(resBuf); });
      }
    }
  }

  function onEnd() { self.destroy(); }
}
util.inherits(StatelessListener, Listener);

/**
 * Stateful transport listener.
 *
 * A handshake is done when the listener is first opened, then all messages are
 * sent without.
 *
 */
function StatefulListener(ptcl, readable, writable, opts) {
  Listener.call(this, ptcl, opts);
  this._readable = readable;
  this._writable = writable;
  this._decoder = undefined;

  var self = this;

  this._readable
    .on('data', onHandshake)
    .on('end', function () { self.destroy(); });

  this._writable
    .on('finish', function () { self.destroy(); });

  function onHandshake(obj) {
    var id = obj.id;
    var err;
    try {
      var parts = readHead(HANDSHAKE_REQUEST_TYPE, obj.payload);
      var hreq = parts.head;
      self._decoder = self._getDecoder(hreq);
      self._readable
        .removeListener('data', onHandshake)
        .on('data', onRequest);
    } catch (cause) {
      err = wrapError('invalid handshake request', cause);
    }
    if (err) {
      done(Buffer.concat([
        new Buffer([0, 1, 0]),
        STRING_TYPE.toBuffer(err.message)
      ]));
    } else {
      self._receive(parts.tail, self._decoder, done);
    }

    function done(resBuf) {
      var hres = self._createHandshakeResponse(err, hreq);
      self.emit('handshake', hreq, hres);
      var payload = Buffer.concat([
        HANDSHAKE_RESPONSE_TYPE.toBuffer(hres),
        resBuf
      ]);
      self._writable.write({id: id, payload: payload});
    }
  }

  function onRequest(obj) {
    // These requests are not prefixed with handshakes.
    var id = obj.id;
    self._receive(obj.payload, self._decoder, function (resBuf, isOneWay) {
      if (!isOneWay) {
        self._writable.write({id: id, payload: resBuf});
      }
    });
  }
}
util.inherits(StatefulListener, Listener);

// Helpers.

/**
 * An Avro message.
 *
 */
function Message(name, attrs, opts) {
  opts = opts || {};
  this._name = name;

  var requestName = 'org.apache.avro.ipc.Request'; // Placeholder name.
  this._requestType = new types.builtins.RecordType({
    name: requestName,
    fields: attrs.request
  }, opts);
  delete opts.registry[requestName];

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

Message.prototype.toJSON = function () {
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

Message.prototype.inspect = Message.prototype.toJSON;

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
      this.push({id: null, payload: Buffer.concat(bufs)});
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
 * @param opts {Object} Options:
 *  + `frameSize` {Number} (Maximum) size in bytes of each frame. The last
 *    frame might be shorter. Defaults to 4096.
 *
 */
function FrameEncoder(opts) {
  stream.Transform.call(this, {writableObjectMode: true});
  this._frameSize = opts && opts.frameSize || 4096;

  this.on('finish', function () { this.push(null); });
}
util.inherits(FrameEncoder, stream.Transform);

FrameEncoder.prototype._transform = function (obj, encoding, cb) {
  var frameSize = this._frameSize;
  var bufs = [obj.payload]; // TODO: Simplify.
  var offset = 0;
  var tail = new Buffer(frameSize + 4);
  var self = this;
  var i, l;
  for (i = 0, l = bufs.length; i < l; i++) {
    frame(bufs[i]);
  }
  if (offset) {
    tail.writeInt32BE(offset);
    this.push(tail.slice(0, offset + 4));
  }
  this.push(intBuffer(0));
  cb();

  function frame(buf) {
    var length = buf.length;
    var start = 0;
    var end = frameSize - offset;

    if (end > length) {
      // The new buffer isn't enough to fill in the partial frame.
      buf.copy(tail, offset + 4);
      offset += length;
      return;
    }

    // The partial frame is now full.
    buf.copy(tail, offset + 4, 0, end);
    tail.writeInt32BE(frameSize);
    self.push(tail);
    tail = new Buffer(frameSize + 4);
    offset = 0;

    // We go through the rest of the buffer.
    while ((start = end) < length) {
      end = start + frameSize;
      if (end > length) {
        // What remains isn't enough to fill a frame.
        buf.copy(tail, 4, start, length);
        offset = length - start;
      } else {
        // Copy a full chunk of the buffer directly into a new frame.
        var framed = new Buffer(frameSize + 4);
        framed.writeInt32BE(frameSize);
        buf.copy(framed, 4, start, end);
        self.push(framed);
      }
    }
  }
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
    this._buf = buf;

    if (this._frameCount) {
      cb();
      return;
    } else {
      var obj = {id: this._id, payload: Buffer.concat(this._bufs)};
      this._bufs = [];
      this._id = undefined;
      this.push(obj);
    }
  }
};

NettyDecoder.prototype._flush = function () {
  if (this._buf._length || this._bufs.length) {
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
  var buf = new Buffer(12);
  buf.writeInt32BE(obj.id, 0);
  buf.writeInt32BE(1, 4);
  buf.writeInt32BE(obj.payload.length, 8);
  this.push(buf);
  this.push(obj.payload);
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
 * Wrap something in an error.
 *
 * @param message {String} The new error's message.
 * @param cause {...} The cause of the error (it can also be an error but
 * doesn't have to). It is available as `cause` field on the outer error.
 *
 * This is used to keep the argument of emitters' `'error'` event errors.
 *
 */
function wrapError(message, cause) {
  var suffix = isError(cause) ? cause.message : cause;
  var err = new Error(f('%s: %s', message, suffix));
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

/**
 * Read prefix.
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
 *
 */
function Decoder(clientPtcl, serverPtcl) {
  serverPtcl = serverPtcl || clientPtcl;
  this._clientPtcl = clientPtcl;
  this._serverPtcl = serverPtcl;
  this._rsvs = clientPtcl.equals(serverPtcl) ? null : this._createResolvers();
}

Decoder.prototype._createResolvers = function () {
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

Decoder.prototype._getReader = function (name, qualifier) {
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

Decoder.prototype.getClientProtocol = function () { return this._clientPtcl; };

Decoder.prototype.getServerProtocol = function () { return this._serverPtcl; };

Decoder.prototype.decodeRequest = function (buf) {
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
    header: hdr,
    message: this._clientPtcl.getMessage(name),
    request: req
  };
};

Decoder.prototype.decodeResponse = function (buf, name) {
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
    header: hdr,
    message: this._serverPtcl.getMessage(name),
    error: err,
    response: res
  };
};

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

Registry.prototype.incr = function () { return this._id = ++this._id | 0; };

Registry.prototype.add = function (timeout, fn) {
  var self = this;
  var id = this.incr();
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

Registry.prototype.get = function (id) { return this._cbs[id]; };

Registry.prototype.clear = function () {
  Object.keys(this._cbs).forEach(function (id) {
    this._cbs[id](new Error('interrupted'));
  }, this);
};


module.exports = {
  HANDSHAKE_REQUEST_TYPE: HANDSHAKE_REQUEST_TYPE,
  HANDSHAKE_RESPONSE_TYPE: HANDSHAKE_RESPONSE_TYPE,
  MAP_BYTES_TYPE: MAP_BYTES_TYPE,
  Message: Message,
  Protocol: Protocol,
  createProtocol: createProtocol,
  Registry: Registry,
  streams: {
    FrameDecoder: FrameDecoder,
    FrameEncoder: FrameEncoder,
    NettyDecoder: NettyDecoder,
    NettyEncoder: NettyEncoder
  }
};
