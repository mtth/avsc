/* jshint node: true */

// TODO: Explore making `MessageEmitter` a writable stream, and
// `MessageListener` a readable stream. The main inconsistency is w.r.t.
// watermarks (the standard stream behavior doesn't support waiting for the
// callbacks, without also preventing concurrent requests).

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


// A few convenience imports.
var Tap = utils.Tap;
var Type = types.Type;
var debug = util.debuglog('avsc');
var f = util.format;

// Various useful types. We instantiate options once, to share the registry.
var OPTS = {namespace: 'org.apache.avro.ipc'};

var BOOLEAN_TYPE = Type.forSchema('boolean', OPTS);

var MAP_BYTES_TYPE = Type.forSchema({type: 'map', values: 'bytes'}, OPTS);

var STRING_TYPE = Type.forSchema('string', OPTS);

var HANDSHAKE_REQUEST_TYPE = Type.forSchema({
  name: 'HandshakeRequest',
  type: 'record',
  fields: [
    {name: 'clientHash', type: {name: 'MD5', type: 'fixed', size: 16}},
    {name: 'clientProtocol', type: ['null', 'string'], 'default': null},
    {name: 'serverHash', type: 'MD5'},
    {name: 'meta', type: ['null', MAP_BYTES_TYPE], 'default': null}
  ]
}, OPTS);

var HANDSHAKE_RESPONSE_TYPE = Type.forSchema({
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

// Prefix used to differentiate between messages when sharing a stream. This
// length should be smaller than 16. The remainder is used for dsambiguating
// between concurrent messages (the current value, 16, therefore supports ~64k
// concurrent messages).
var PREFIX_LENGTH = 16;

var PING_MESSAGE = new Message(
  '',
  Type.forSchema({name: 'PingRequest', type: 'record', fields: []}, OPTS),
  Type.forSchema(['string'], OPTS),
  Type.forSchema('null', OPTS),
  true
);

/** An Avro message, containing its request, response, etc. */
function Message(name, reqType, errType, resType, isOneWay, doc) {
  this._name = name;
  if (!Type.isType(reqType, 'record')) {
    throw new Error('invalid request type');
  }
  this._reqType = reqType;
  if (
    !Type.isType(errType, 'union') ||
    !Type.isType(errType.getTypes()[0], 'string')
  ) {
    throw new Error('invalid error type');
  }
  this._errType = errType;
  if (isOneWay) {
    if (!Type.isType(resType, 'null') || errType.getTypes().length > 1) {
      throw new Error('unapplicable one-way parameter');
    }
  }
  this._resType = resType;
  this._oneWay = isOneWay;
  this._doc = doc;
}

Message.forSchema = function (name, schema, opts) {
  opts = opts || {};

  if (!types.isValidName(name)) {
    throw new Error(f('invalid message name: %s', name));
  }

  var recordName = f('org.apache.avro.ipc.%sRequest', utils.capitalize(name));
  var reqType = Type.forSchema({
    name: recordName,
    type: 'record',
    namespace: opts.namespace || '', // Don't leak request namespace.
    fields: schema.request
  }, opts);
  // We remove the record from the registry to prevent it from being exported
  // in the protocol's schema.
  delete opts.registry[recordName];

  if (!schema.response) {
    throw new Error('missing response');
  }
  var resType = Type.forSchema(schema.response, opts);

  var errors = schema.errors || [];
  errors.unshift('string');
  var errType = Type.forSchema(errors, opts);

  var isOneWay = !!schema['one-way'];
  var doc = schema.doc ? '' + schema.doc : undefined;
  return new Message(name, reqType, errType, resType, isOneWay, doc);
};

Message.prototype.getDocumentation = function () { return this._doc; };

Message.prototype.getName = function () { return this._name; };

Message.prototype.getRequestType = function () { return this._reqType; };

Message.prototype.getResponseType = function () { return this._resType; };

Message.prototype.getErrorType = function () { return this._errType; };

Message.prototype.isOneWay = function () { return this._oneWay; };

Message.prototype.isPing = function () { return !this._name.length; };

Message.prototype.inspect = Message.prototype.toJSON = function () {
  var obj = {
    request: this._reqType.getFields(),
    response: this._resType,
    doc: this._doc
  };
  var errorTypes = this._errType.getTypes();
  if (errorTypes.length > 1) {
    obj.errors = errorTypes.slice(1);
  }
  if (this._oneWay) {
    obj['one-way'] = true;
  }
  return obj;
};

Message.prototype.toString = function () {
  return types.stringify(this);
};

function WrappedRequest(msg, hdr, req) {
  this._msg = msg;
  this._hdr = hdr || {};
  this._req = req || {};
}

WrappedRequest.fromArgs = function (msg, args) {
  var Record = msg.getRequestType().getRecordConstructor();
  var req = new (Record.bind.apply(Record, [undefined].concat(args)))();
  return new WrappedRequest(msg, {}, req);
};

WrappedRequest.prototype.getMessage = function () { return this._msg; };

WrappedRequest.prototype.getHeader = function () { return this._hdr; };

WrappedRequest.prototype.getRequest = function () { return this._req; };

WrappedRequest.prototype.toBuffer = function () {
  var msg = this._msg;
  return Buffer.concat([
    MAP_BYTES_TYPE.toBuffer(this._hdr),
    STRING_TYPE.toBuffer(msg.getName()),
    msg.getRequestType().toBuffer(this._req)
  ]);
};

function WrappedResponse(msg, hdr, err, res) {
  this._msg = msg;
  this._hdr = hdr;
  this._err = err;
  this._res = res;
}

WrappedResponse.prototype.hasError = function () {
  return this._err !== undefined;
};

WrappedResponse.prototype.getMessage = function () { return this._msg; };

WrappedResponse.prototype.getHeader = function () { return this._hdr; };

WrappedResponse.prototype.getError = function () { return this._err; };

WrappedResponse.prototype.getResponse = function () { return this._res; };

WrappedResponse.prototype.setError = function (err) {
  this._err = err;
  this._res = undefined;
};

WrappedResponse.prototype.setResponse = function (res) {
  this._err = undefined;
  this._res = res;
};

WrappedResponse.prototype.toBuffer = function () {
  var hdr = MAP_BYTES_TYPE.toBuffer(this._hdr);
  var hasError = this.hasError();
  return Buffer.concat([
    hdr,
    BOOLEAN_TYPE.toBuffer(hasError),
    hasError ?
      this._msg.getErrorType().toBuffer(this._err) :
      this._msg.getResponseType().toBuffer(this._res)
  ]);
};

/**
 * An Avro protocol.
 *
 * This constructor shouldn't be called directly, but via the
 * `Protocol.forSchema` method. This function performs little logic to better
 * support efficient protocol copy.
 */
function Protocol(name, messages, types, doc, server) {
  if (typeof name != 'string') {
    // Let's be helpful in case this class is instantiated directly.
    return Protocol.forSchema(name, messages);
  }

  this._name = name;
  this._messages = messages || {};
  this._types = types || {};
  // We cache a string rather than a buffer to not retain an entire slab. This
  // also lets us use hashes as keys inside maps (e.g. for resolvers). It is
  // populated lazily the first time `getFingerprint` is called.
  this._hs = undefined;
  this._doc = doc ? '' + doc : undefined;

  // Backwards-compatilibity.
  this._server = server || this.createServer();
}

Protocol.forSchema = function (schema, opts) {
  opts = opts || {};

  var name = schema.protocol;
  if (!name) {
    throw new Error('missing protocol name');
  }
  if (schema.namespace !== undefined) {
    opts.namespace = schema.namespace;
  } else {
    var match = /^(.*)\.[^.]+$/.exec(name);
    if (match) {
      opts.namespace = match[1];
    }
  }
  name = types.qualify(name, opts.namespace);

  if (schema.types) {
    schema.types.forEach(function (obj) { Type.forSchema(obj, opts); });
  }

  var messages = {};
  if (schema.messages) {
    Object.keys(schema.messages).forEach(function (key) {
      messages[key] = Message.forSchema(key, schema.messages[key], opts);
    });
  }
  return new Protocol(name, messages, opts.registry, schema.doc);
};

Protocol.isProtocol = function (any) {
  // Not fool-proof but likely sufficient.
  return (
    !!any &&
    any.hasOwnProperty('_hs') &&
    typeof any.getFingerprint == 'function'
  );
};

Protocol.prototype.getDocumentation = function () { return this._doc; };

Protocol.prototype.getFingerprint = Type.prototype.getFingerprint;

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
  var schemaStr = types.stringify({
    protocol: this._name,
    doc: (opts && opts.exportAttrs) ? this._doc : undefined,
    types: namedTypes.length ? namedTypes : undefined,
    messages: Object.keys(this._messages).length ? this._messages : undefined
  }, opts);
  return (opts && opts.asString) ? schemaStr : JSON.parse(schemaStr);
};

Protocol.prototype.equals = function (ptcl) {
  return (
    Protocol.isProtocol(ptcl) &&
    this.getFingerprint().equals(ptcl.getFingerprint())
  );
};

Protocol.prototype.toString = function () {
  return this.getSchema({asString: true, noDeref: true});
};

Protocol.prototype.inspect = function () {
  return f('<Protocol %j>', this._name);
};

Protocol.prototype.createClient = function (opts) {
  var client = new Client(this, opts);
  if (opts && opts.transport) {
    // Convenience function for the common single emitter use-case.
    client.createEmitter(opts.transport);
  }
  return client;
};

Protocol.prototype.createServer = function (opts) {
  return new Server(this, opts);
};

Protocol.prototype.subprotocol = util.deprecate(
  function () {
    return new Protocol(
      this._name,
      this._messages,
      this._types,
      this._doc,
      this._server.extend()
    );
  },
  'use `protocol.createServer` and `server.extend` instead'
);

Protocol.prototype.createEmitter = util.deprecate(
  function (transport, opts) {
    return this.createClient(opts).createEmitter(transport, opts);
  },
  'use `protocol.createClient` instead'
);

Protocol.prototype.createListener = util.deprecate(
  function (transport, opts) {
    if (opts && opts.strictErrors) {
      // TODO: Doc.
      throw new Error('use `protocol.createServer` to support strict errors');
    }
    return this._server.createListener(transport, opts);
  },
  'use `protocol.createServer` and `server.createListener` instead'
);

Protocol.prototype.emit = util.deprecate(
  function (name, req, emitter, cb) {
    if (!emitter || !this.equals(emitter.getProtocol())) {
      throw new Error('invalid emitter');
    }

    var client = emitter.getClient();
    // In case the method is overridden.
    Client.prototype.emitMessage.call(client, name, req, cb && cb.bind(this));
    return emitter.getPending();
  },
  'use methods on a client instead'
);

Protocol.prototype.on = util.deprecate(
  function (name, handler) {
    var self = this; // This protocol.
    this._server.onMessage(name, function (req, cb) {
      return handler.call(self, req, this, cb);
    });
    return this;
  },
  'use `server.onMessage` instead'
);

Protocol.prototype.getHandler = util.deprecate(
  function (name) {
    var handler = this._server._handlers[name];
    if (!handler) {
      return undefined;
    }
    return function (req, ee, cb) {
      return handler.call(ee, req, cb);
    };
  },
  'server middleware does not require direct handler access'
);

function discoverProtocolSchema(transport, opts, cb) {
  if (cb === undefined && typeof opts == 'function') {
    cb = opts;
    opts = undefined;
  }

  var ptcl = Protocol.forSchema({protocol: 'Empty'});
  var client = ptcl.createClient();
  var emitter = client.createEmitter(transport, {
    scope: opts && opts.scope,
    timeout: opts && opts.timeout,
    endWritable: typeof transport == 'function' // Stateless transports only.
  });
  emitter
    .once('handshake', function (hreq, hres) {
      this.destroy(true); // Prevent further requests on this emitter.
      cb(null, JSON.parse(hres.serverProtocol));
    })
    .on('error', function (err) {
      // Stateless transports will throw an interrupted error when the emitter
      // is destroyed, we ignore it here.
      if (!/interrupted/.test(err)) {
        cb(err); // Likely timeout.
      }
    });
}

function Client(ptcl, opts) {
  opts = opts || {};

  // We have to suffix these properties to be safe, since the message names
  // aren't prefixed with clients (unlike servers).
  this._ptcl$ = ptcl;
  this._emitters$ = []; // Active emitters.
  this._fns$ = []; // Middleware functions.
  this._cache$ = opts.cache || {};
  this._policy$ = opts.policy;
  this._strict$ = !!opts.strictErrors;

  this._ptcl$.getMessages().forEach(function (msg) {
    this[msg.getName()] = this._createMessageHandler(msg);
  }, this);
}

Client.prototype.getEmitters = function () {
  return this._emitters$.slice();
};

Client.prototype.getProtocol = function () { return this._ptcl$; };

Client.prototype.createEmitter = function (transport, opts) {
  opts = opts || {};
  opts.cache = opts.cache || this._cache$;

  var objectMode = opts.objectMode;
  var emitter;
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
    emitter = new StatelessEmitter(this, writableFactory, opts);
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
      var decoder = new NettyDecoder();
      readable = readable.pipe(decoder);
      var encoder = new NettyEncoder();
      encoder.pipe(writable);
      writable = encoder;
    }
    emitter = new StatefulEmitter(this, readable, writable, opts);
    if (!objectMode) {
      // Since we never expose the encoder and decoder, we must release them
      // ourselves here.
      emitter.once('eot', function () {
        readable.unpipe(decoder);
        encoder.unpipe(writable);
      });
    }
  }
  var emitters = this._emitters$;
  emitters.push(emitter);
  emitter.once('eot', function () {
    var pos = emitters.indexOf(this);
    emitters.splice(pos, 1);
  });
  return emitter;
};

Client.prototype.emitMessage = function (name, req, opts, cb) {
  if (!cb && typeof opts === 'function') {
    cb = opts;
    opts = undefined;
  }
  var msg = getExistingMessage(this._ptcl$, name);
  var wreq = new WrappedRequest(msg, {}, req);
  return Client.prototype._emitMessage.call(this, wreq, opts, cb);
};

Client.prototype.use = function (fn) {
  this._fns$.push(fn);
  return this;
};

Client.prototype._emitMessage = function (wreq, opts, cb) {
  // Common logic between `client.emitMessage` and the "named" message methods.
  var self = this;

  // Select which emitter to use.
  var emitters = this._emitters$;
  var numEmitters = emitters.length;
  if (!numEmitters) {
    var err = new Error('no emitters available');
    if (cb) {
      cb(err);
      return;
    } else {
      throw err; // TODO: If client is an event emitter, emit it instead.
    }
  }
  var emitter;
  if (numEmitters === 1) {
    // Common case, optimized away.
    emitter = emitters[0];
  } if (this._policy$) {
    emitter = this._policy$(this._emitters$.slice());
  } else {
    // Random selection, cheap and likely
    emitter = emitters[Math.floor(Math.random() * numEmitters)];
  }

  chainFns(this._fns$, emitter, [wreq], target, source);
  // TODO: Return useful value here.

  function target(wreq, cb) { emitter._emit(wreq, opts, cb); }

  function source(err, wres) {
    var errType = wreq.getMessage().getErrorType();
    // System error, likely the message wasn't sent (or an error occurred
    // while decoding the response).
    if (err) {
      if (self._strict$) {
        err = errType.clone(err.message, {wrapUnions: true});
      }
      done(err);
      return;
    }
    // Message transmission succeeded, we transmit the message data;
    // massaging any error strings into actual `Error` objects in non-strict
    // mode.
    err = wres.getError();
    if (!self._strict$) {
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
    done(err, wres.getResponse());
  }

  function done(err, res) {
    if (cb) {
      cb.call(emitter, err, res);
    } else if (err) {
      emitter.emit('error', err);
    }
  }
};

Client.prototype._createMessageHandler = function (msg) {
  return function (/* fields ... , [opts,] [cb] */) {
    // TODO: Code generate this function.
    var args = [];
    var i, l;
    for (i = 0, l = arguments.length; i < l; i++) {
      args.push(arguments[i]);
    }
    var type = msg.getRequestType();
    var numFields = type.getFields().length;
    var opts, cb;
    if (l === numFields + 2) {
      cb = args.pop();
      opts = args.pop();
    } else if (l === numFields + 1) {
      var arg = args.pop();
      if (typeof arg === 'function') {
        cb = arg;
      } else {
        opts = arg;
      }
    }
    var wreq = WrappedRequest.fromArgs(msg, args);
    return Client.prototype._emitMessage.call(this, wreq, opts, cb);
  };
};

/** Listener creator. */
function Server(ptcl, opts) {
  opts = opts || {};

  this._ptcl = ptcl;
  this._handlers = {};
  this._fns = []; // Middleware functions.
  this._listeners = {}; // Active listeners.
  this._nextListenerId = 1;
  this._strict = !!opts.strictErrors;
  this._cache = opts.cache || {};

  ptcl.getMessages().forEach(function (message) {
    var name = message.getName();
    if (!opts.noCapitalize) {
      name = utils.capitalize(name);
    }
    this['on' + name] = this._createMessageHandler(message);
  }, this);
}

Server.prototype.getListeners = function () {
  var listeners = this._listeners;
  return Object.keys(listeners).map(function (id) { return listeners[id]; });
};

Server.prototype.getProtocol = function () { return this._ptcl; };

Server.prototype.createListener = function (transport, opts) {
  opts = opts || {};
  opts.cache = opts.cache || this._cache;

  var listener;
  if (typeof transport == 'function') {
    var readableFactory;
    if (opts.objectMode) {
      readableFactory = transport;
    } else {
      readableFactory = function (cb) {
        return transport(function (err, writable) {
          if (err) {
            cb(err);
            return;
          }
          var encoder = new FrameEncoder();
          encoder.pipe(writable);
          cb(null, encoder);
        }).pipe(new FrameDecoder());
      };
    }
    listener = new StatelessListener(this, readableFactory, opts);
  } else {
    var readable, writable;
    if (isStream(transport)) {
      readable = writable = transport;
    } else {
      readable = transport.readable;
      writable = transport.writable;
    }
    if (!opts.objectMode) {
      var decoder = new NettyDecoder();
      readable = readable.pipe(decoder);
      var encoder = new NettyEncoder();
      encoder.pipe(writable);
      writable = encoder;
    }
    listener = new StatefulListener(this, readable, writable, opts);
    if (!opts.objectMode) {
      // Similar to emitters, since we never expose the encoder and decoder, we
      // must release them ourselves here.
      listener.once('eot', function () {
        readable.unpipe(decoder);
        encoder.unpipe(writable);
      });
    }
  }

  var listenerId = this._nextListenerId++;
  var listeners = this._listeners;
  listeners[listenerId] = listener;
  listener.once('eot', function () { delete listeners[listenerId]; });
  // TODO: Emit 'listener' event on server.
  return listener;
};

Server.prototype.callback = function (opts, fn) {
  if (fn === undefined && typeof opts == 'function') {
    fn = opts;
    opts = undefined;
  }

  return function () {
    var transport;
    if (fn) {
      transport = fn.apply(undefined, arguments);
    } else {
      // Provide a few defaults for common use-cases.
      switch (arguments.length) {
        case 1: // TCP-connection.
          var duplex = arguments[0];
          // TODO: Check if stream.
          transport = duplex;
          break;
        case 2: // Http server.
        case 3: // Express.
          var readable = arguments[0];
          var writable = arguments[1];
          // TODO: Check if stream.
          transport = function (cb) {
            cb(null, writable);
            return readable;
          };
          break;
      }
    }
    if (!transport) {
      throw new Error('invalid transport');
    }
    this.createListener(transport, opts);
  };
};

Server.prototype.extend = function () {
  // It only makes sense for the child server to have the same options as the
  // parent (otherwise the handlers would fail when called for example).
  var opts = {strictErrors: this._strict, cache: this._cache};
  var server = new Server(this._ptcl, opts);
  server._handlers = Object.create(this._handlers);
  return server;
};

Server.prototype.onMessage = function (name, handler) {
  var msg = getExistingMessage(this.getProtocol(), name);
  this._handlers[name] = (msg.isOneWay() || this._strict) ?
    handler :
    function (req, cb) {
      var listener = this;
      return handler.call(listener, req, function (err, res) {
        // Allow `Error` objects or `null` to be passed in as argument in
        // non-strict mode, even though these are likely not valid Avro types
        // (since these are idiomatic JavaScript).
        if (isError(err)) {
          err = msg.getErrorType().clone(err.message, {wrapUnions: true});
        } else if (err === null) {
          err = undefined;
        }
        cb(err, res);
      });
    };
  return this;
};

Server.prototype.use = function (fn) {
  this._fns.push(fn);
  return this;
};

Server.prototype._createMessageHandler = function (message) {
  // TODO: Code-generate the parameter expansion.
  return function (handler) {
    return this.onMessage(message.getName(), function (req, cb) {
      var requestType = message.getRequestType();
      var args = requestType.getFields().map(function (field) {
        return req[field.getName()];
      });
      args.push(cb);
      handler.apply(this, args); // Context is the listener.
    });
  };
};

/** Base message emitter class. See below for the two available variants. */
function MessageEmitter(client, opts) {
  opts = opts || {};
  events.EventEmitter.call(this);

  this._client = client;
  this._endWritable = !!utils.getOption(opts, 'endWritable', true);
  this._timeout = utils.getOption(opts, 'timeout', 10000);
  this._prefix = normalizedPrefix(opts.scope);

  this._cache = opts.cache || {};
  var ptcl = client.getProtocol();
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

  this._registry = new Registry(this, PREFIX_LENGTH);
  this._destroyed = false;
  this._interrupted = false;
  this.once('_eot', function (pending) { this.emit('eot', pending); });
}
util.inherits(MessageEmitter, events.EventEmitter);

MessageEmitter.prototype.getCache = function () { return this._cache; };

MessageEmitter.prototype.getClient = function () { return this._client; };

MessageEmitter.prototype.getProtocol = function () {
  return Client.prototype.getProtocol.call(this._client);
};

MessageEmitter.prototype.getTimeout = function () { return this._timeout; };

MessageEmitter.prototype.isDestroyed = function () { return this._destroyed; };

MessageEmitter.prototype.getPending = function () {
  return this._registry.size();
};

MessageEmitter.prototype._emit = function (wreq, opts, cb) {
  if (cb === undefined && typeof opts == 'function') {
    cb = opts;
    opts = undefined;
  }
  if (typeof cb != 'function') {
    throw new Error(f('invalid callback: %s', cb));
  }

  // Serialize the message.
  var err, reqBuf;
  if (this._destroyed) {
    err = new Error('destroyed');
  } else {
    try {
      reqBuf = wreq.toBuffer();
    } catch (cause) {
      err = wrapError('invalid request', cause);
    }
  }

  // Return now if a serialization error occurred.
  var self = this;
  if (err) {
    process.nextTick(function () { cb.call(self, err); });
    return true;
  }

  // Generate the response callback.
  var msg = wreq.getMessage();
  var timeout = (opts && opts.timeout !== undefined) ?
    opts.timeout :
    this._timeout;
  var id = this._registry.add(timeout, function (err, resBuf, adapter) {
    var wres;
    if (!err) {
      try {
        wres = adapter.decodeResponse(resBuf, msg.getName());
      } catch (cause) {
        err = wrapError('invalid response', cause);
      }
    }
    cb.call(this, err, wres);
    if (this._destroyed && !this._interrupted && !this._registry.size()) {
      this.destroy();
    }
  });

  id |= this._prefix;
  debug('sending message %s', id);
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
  var ptcl = this.getProtocol();
  return {
    clientHash: ptcl.getFingerprint(),
    clientProtocol: noPtcl ?
      null :
      ptcl.getSchema({asString: true, exportAttrs: true}),
    serverHash: adapter._fingerprint
  };
};

MessageEmitter.prototype._getAdapter = function (hres) {
  var serverBuf = hres.serverHash;
  var adapter = this._cache[serverBuf];
  if (adapter) {
    return adapter;
  }
  var serverPtcl = Protocol.forSchema(
    JSON.parse(hres.serverProtocol),
    {wrapUnions: true}
    // Wrapping is required to support all schemas, but has no effect on the
    // final output (controlled by the server's protocol) since resolution
    // is independent of whether unions are wrapped or not.
  );
  adapter = new Adapter(this.getProtocol(), serverPtcl, serverBuf);
  return this._cache[serverBuf] = adapter;
};

MessageEmitter.prototype._matchesPrefix = function (id) {
  return matchesPrefix(id, this._prefix);
};

MessageEmitter.prototype.emitMessage = util.deprecate(
  function (name, reqEnv, opts, cb) {
    if (cb === undefined && typeof opts == 'function') {
      cb = opts;
      opts = undefined;
    }
    if (!cb) {
      throw new Error('missing callback');
    }

    var ptcl = this.getProtocol();
    var msg = ptcl.getMessage(name);
    if (!msg) {
      // Backwards-compatibility.
      process.nextTick(function () {
        cb.call(ptcl, new Error(f('unknown message: %s', name)));
      });
      return;
    }

    var wreq = new WrappedRequest(msg, reqEnv.header, reqEnv.request);
    return this._emit(wreq, opts, function (err, wres) {
      var env;
      if (!err) {
        env = {
          header: wres.getHeader(),
          error: wres.getError(),
          response: wres.getResponse()
        };
      }
      cb.call(this, err, env);
    });
  },
  'use `Client.emitMessage` instead'
);

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
 */
function StatelessEmitter(client, writableFactory, opts) {
  MessageEmitter.call(this, client, opts);
  this._writableFactory = writableFactory;

  if (!opts || !opts.noPing) {
    // Ping the server to check whether the remote protocol is compatible.
    var wreq = new WrappedRequest(PING_MESSAGE, {}, {});
    this._emit(wreq, function (err) {
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
    if (self._interrupted) {
      // The request's callback will already have been called.
      return;
    }

    var hreq = self._createHandshakeRequest(adapter, !retry);

    var writable = self._writableFactory.call(self, function (err, readable) {
      if (err) {
        cb(err);
        return;
      }
      readable.on('data', function (obj) {
        debug('received response %s', obj.id);
        // We don't check that the prefix matches since the ID likely hasn't
        // been propagated to the response (see default stateless codec).
        var buf = Buffer.concat(obj.payload);
        try {
          var parts = readHead(HANDSHAKE_RESPONSE_TYPE, buf);
          var hres = parts.head;
          if (hres.serverHash) {
            adapter = self._getAdapter(hres);
          }
        } catch (err) {
          cb(err);
          return;
        }
        self.emit('handshake', hreq, hres);
        if (hres.match === 'NONE') {
          process.nextTick(function() { emit(true); });
          return;
        }
        // Change the default adapter.
        self._adapter = adapter;
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
 */
function StatefulEmitter(client, readable, writable, opts) {
  MessageEmitter.call(this, client, opts);
  this._readable = readable;
  this._writable = writable;
  this._connected = !!(opts && opts.noPing);
  this._readable.on('end', onEnd);
  this._writable.on('finish', onFinish);

  var timer = null;
  this.once('eot', function () {
    debug('emitter eot');
    if (timer) {
      clearTimeout(timer);
      timer = null;
    }
    // Remove references to this emitter to avoid potential memory leaks.
    this._writable.removeListener('finish', onFinish);
    if (this._endWritable) {
      debug('ending transport');
      this._writable.end();
    }
    this._readable
      .removeListener('data', onPing)
      .removeListener('data', onMessage)
      .removeListener('end', onEnd);
  });

  var self = this;
  var hreq; // For handshake events.
  if (this._connected) {
    this._readable.on('data', onMessage);
  } else {
    this._readable.on('data', onPing);
    process.nextTick(ping);
    if (self._timeout) {
      timer = setTimeout(function () {
        self.destroy(true);
        self.emit('error', new Error('connection timeout'));
      }, self._timeout);
    }
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
    self._writable.write({id: self._prefix, payload: payload});
  }

  function onPing(obj) {
    if (!self._matchesPrefix(obj.id)) {
      debug('discarding unscoped response %s (still connecting)', obj.id);
      return;
    }
    var buf = Buffer.concat(obj.payload);
    try {
      var hres = readHead(HANDSHAKE_RESPONSE_TYPE, buf).head;
      if (hres.serverHash) {
        self._adapter = self._getAdapter(hres);
      }
    } catch (err) {
      self.destroy(true); // Not a recoverable error.
      self.emit('error', wrapError('handshake error (this can happen if the ' +
        'underlying transport is shared by multiple emitters or listeners ' +
        'using the same scope)', err));
      return;
    }
    self.emit('handshake', hreq, hres);
    if (hres.match === 'NONE') {
      process.nextTick(function () { ping(true); });
    } else {
      debug('successfully connected');
      if (timer) {
        clearTimeout(timer);
        timer = null;
      }
      self._readable.removeListener('data', onPing).on('data', onMessage);
      self._connected = true;
      self.emit('_connected');
      hreq = null; // Release reference.
    }
  }

  // Callback used after a connection has been established.
  function onMessage(obj) {
    var id = obj.id;
    if (!self._matchesPrefix(id)) {
      debug('discarding unscoped message %s', id);
      return;
    }
    var cb = self._registry.get(id);
    if (cb) {
      process.nextTick(function () {
        debug('received message %s', id);
        // Ensure that the initial callback gets called asynchronously, even
        // for completely synchronous transports (otherwise the number of
        // pending requests will sometimes be inconsistent between stateful and
        // stateless transports).
        cb(null, Buffer.concat(obj.payload), self._adapter);
      });
    }
  }

  function onEnd() { self.destroy(true); }
  function onFinish() { self.destroy(); }
}
util.inherits(StatefulEmitter, MessageEmitter);

StatefulEmitter.prototype._send = function (id, reqBuf, isOneWay) {
  if (!this._connected) {
    debug('queuing request %s', id);
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

/** The server-side emitter equivalent. */
function MessageListener(server, opts) {
  opts = opts || {};
  events.EventEmitter.call(this);

  this._server = server;
  this._endWritable = !!utils.getOption(opts, 'endWritable', true);
  this._prefix = normalizedPrefix(opts.scope);

  this._cache = opts.cache || {};
  var ptcl = this._server.getProtocol();
  var fgpt = ptcl.getFingerprint();
  if (!this._cache[fgpt]) {
    // Add the listener's protocol to the cache if it isn't already there. This
    // will save a handshake the first time on emitters with the same protocol.
    this._cache[fgpt] = new Adapter(ptcl, ptcl, fgpt);
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

MessageListener.prototype.getProtocol = function () {
  return this._server.getProtocol();
};

MessageListener.prototype.getServer = function () { return this._server; };

MessageListener.prototype.isDestroyed = function () {
  return this._destroyed;
};

MessageListener.prototype.destroy = function (noWait) {
  this._destroyed = true;
  if (noWait || !this._pending) {
    this._interrupted = true;
    this.emit('_eot', this._pending);
  }
};

MessageListener.prototype._receive = function (reqBuf, adapter, cb) {
  var self = this;
  var wreq;
  try {
    wreq = adapter.decodeRequest(reqBuf);
  } catch (err) {
    cb(encodeError(err));
    return;
  }

  var msg = wreq.getMessage();
  if (!msg.getName()) {
    // Ping message, we don't invoke middleware logic in this case.
    cb((new WrappedResponse(msg, {}, undefined, null)).toBuffer(), false);
    return;
  }

  var isDone = false;
  chainFns(this._server._fns, this, [wreq], target, source);

  function target(wreq, cb) {
    self._pending++;
    if (self._hook) {
      // Legacy hook.
      var eq = {header: wreq.getHeader(), request: wreq.getRequest()};
      self._hook.call(self, msg.getName(), eq, function (err, es) {
        cb(null, new WrappedResponse(msg, es.header, es.error, es.response));
      });
    } else {
      var handler = self._server._handlers[msg.getName()];
      if (!handler) {
        // The underlying protocol hasn't implemented a handler.
        cb(new Error(f('unhandled message: %s', msg.getName())));
      } else if (msg.isOneWay()) {
        handler.call(self, wreq.getRequest());
        cb(null, new WrappedResponse(msg, {}));
      } else {
        try {
          handler.call(self, wreq.getRequest(), function (err, res) {
            cb(null, new WrappedResponse(msg, {}, err, res));
          });
        } catch (err) {
          // We catch synchronous failures (same as express) and return the
          // failure. Note that the server process can still crash if an error
          // is thrown after the handler returns but before the response is
          // sent (again, same as express). We don't do this for one-way
          // messages because potential errors would then easily pass
          // unnoticed.
          cb(err);
        }
      }
    }
  }

  function source(err, wres) {
    if (isDone) {
      self.emit('error', new Error('message callback called multiple times'));
      return;
    }
    isDone = true;
    self._pending--;
    var resBuf;
    if (!err) {
      try {
        resBuf = wres.toBuffer();
      } catch (cause) {
        err = wrapError('invalid response', cause);
      }
    }
    if (err) {
      resBuf = encodeError(err, wres ? wres.getHeader() : undefined);
    }
    if (!self._interrupted) {
      cb(resBuf, msg.isOneWay());
    }
    if (self._destroyed && !self._pending) {
      self.destroy();
    }
  }
};

MessageListener.prototype._createHandshakeResponse = function (err, hreq) {
  var ptcl = this.getProtocol();
  var buf = ptcl.getFingerprint();
  var serverMatch = hreq && hreq.serverHash.equals(buf);
  return {
    match: err ? 'NONE' : (serverMatch ? 'BOTH' : 'CLIENT'),
    serverProtocol: serverMatch ?
      null :
      ptcl.getSchema({asString: true, exportAttrs: true}),
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
  var clientPtcl = Protocol.forSchema(
    JSON.parse(hreq.clientProtocol),
    {wrapUnions: true} // See `MessageEmitter._getAdapter`.
  );
  adapter = new Adapter(clientPtcl, this.getProtocol(), clientBuf);
  return this._cache[clientBuf] = adapter;
};

MessageListener.prototype._matchesPrefix = function (id) {
  return matchesPrefix(id, this._prefix);
};

MessageListener.prototype.onMessage = util.deprecate(
  function (fn) {
    this._hook = fn;
    return this;
  },
  'use `server.use` to implement middleware instead'
);

/**
 * MessageListener for stateless transport.
 *
 * This listener expect a handshake to precede each message.
 */
function StatelessListener(server, readableFactory, opts) {
  MessageListener.call(this, server, opts);
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
 */
function StatefulListener(server, readable, writable, opts) {
  MessageListener.call(this, server, opts);
  this._adapter = undefined;
  this._writable = writable.on('finish', onFinish);
  this._readable = readable.on('data', onHandshake).on('end', onEnd);

  this.once('eot', function () {
    // Clean up any references to the listener on the underlying streams.
    this._writable.removeListener('finish', onFinish);
    if (this._endWritable) {
      this._writable.end();
    }
    this._readable
      .removeListener('data', onHandshake)
      .removeListener('data', onRequest)
      .removeListener('end', onEnd);
  });

  var self = this;

  function onHandshake(obj) {
    var id = obj.id;
    if (!self._matchesPrefix(id)) {
      return;
    }
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
    if (!self._matchesPrefix(id)) {
      return;
    }
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

// Helpers.

/**
 * Callback registry.
 *
 * Callbacks added must accept an error as first argument. This is used by
 * message emitters to store pending calls.
 *
 */
function Registry(ctx, prefixLength) {
  this._ctx = ctx; // Context for all callbacks.
  this._mask = ~0 >>> (prefixLength | 0); // 16 bits by default.
  this._id = 0; // Unique integer ID for each call.
  this._n = 0; // Number of pending calls.
  this._cbs = {};
}

Registry.prototype.size = function () { return this._n; };

Registry.prototype.get = function (id) { return this._cbs[id & this._mask]; };

Registry.prototype.add = function (timeout, fn) {
  this._id = (this._id + 1) & this._mask;

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
  var msg, req;
  if (name) {
    msg = this._serverPtcl.getMessage(name);
    req = this._getReader(name, '?')._read(tap);
  } else {
    msg = PING_MESSAGE;
  }
  if (!tap.isValid()) {
    throw new Error('truncated request');
  }
  return new WrappedRequest(msg, hdr, req);
};

Adapter.prototype.decodeResponse = function (buf, name) {
  var tap = new Tap(buf);
  var hdr = MAP_BYTES_TYPE._read(tap);
  var isError = BOOLEAN_TYPE._read(tap);
  var msg, err, res;
  if (name) {
    var reader = this._getReader(name, isError ? '*' : '!');
    msg = this._clientPtcl.getMessage(name);
    if (isError) {
      err = reader._read(tap);
    } else {
      res = reader._read(tap);
    }
  } else {
    msg = PING_MESSAGE;
  }
  if (!tap.isValid()) {
    throw new Error('truncated response');
  }
  return new WrappedResponse(msg, hdr, err, res);
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
    var bufs = this._bufs.slice();
    bufs.unshift(this._buf);
    this.emit('error', new Error('trailing data: ' + Buffer.concat(bufs)));
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
  header = header || {};
  header.stackTrace = err.stack; // TODO: Find most compatible way.

  var hdrBuf;
  try {
    // Propagate the header if possible.
    hdrBuf = MAP_BYTES_TYPE.toBuffer(header);
  } catch (err) {
    hdrBuf = new Buffer([0]);
  }
  return Buffer.concat([
    hdrBuf,
    new Buffer([1, 0]), // Error flag and first union index.
    STRING_TYPE.toBuffer(err.message)
  ]);
}

/**
 * Compute a prefix of fixed length from a string.
 *
 * @param scope {String} Namespace to be hashed.
 *
 */
function normalizedPrefix(scope) {
  return scope ?
    utils.getHash(scope).readInt16BE(0) << (32 - PREFIX_LENGTH) :
    0;
}

/**
 * Check whether an ID matches the prefix.
 *
 * @param id {Integer} Number to check.
 * @param prefix {Integer} Already shifted prefix.
 *
 */
function matchesPrefix(id, prefix) {
  return ((id ^ prefix) >> (32 - PREFIX_LENGTH)) === 0;
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
  return !!(any && any.pipe);
}

/** Get a message, asserting that it exists. */
function getExistingMessage(ptcl, name) {
  var msg = ptcl.getMessage(name);
  if (!msg) {
    throw new Error(f('unknown message: %s', name));
  }
  return msg;
}

/** Middleware logic. */
function chainFns(fns, ctx, args, target, source) {
  var cbs = [];
  forward(0);

  function forward(pos) {
    var isDone = false;
    if (pos < fns.length) {
      fns[pos].apply(ctx, args.concat(function (err, cb) {
        if (isDone) {
          err = new Error('duplicate middleware forward call');
        }
        isDone = true;
        if (err) {
          source.call(ctx, err);
          return;
        }
        if (cb) {
          cbs.push(cb);
        }
        forward(++pos);
      }));
    } else {
      // Out of middleware functions, start "back-propagation".
      target.apply(ctx, args.concat(function () {
        if (isDone) {
          err = new Error('duplicate handler call');
        }
        isDone = true;
        var numArgs = arguments.length;
        var err;
        if (!numArgs) {
          err = null;
        } else {
          err = arguments[0];
          if (err) {
            source.call(ctx, err);
            return;
          }
        }
        args = []; // Reset arguments.
        var i = 1;
        while (i < numArgs) {
          args.push(arguments[i++]);
        }
        backward();
      }));
    }
  }

  function backward() {
    var isDone = false;
    var cb = cbs.pop();
    if (cb) {
      cb.apply(ctx, args.concat(function (err) {
        if (isDone) {
          err = new Error('duplicate middleware backward call');
        }
        isDone = true;
        if (err) {
          source.call(ctx, err);
          return;
        }
        backward();
      }));
    } else {
      source.apply(ctx, [null].concat(args));
    }
  }
}


module.exports = {
  HANDSHAKE_REQUEST_TYPE: HANDSHAKE_REQUEST_TYPE,
  HANDSHAKE_RESPONSE_TYPE: HANDSHAKE_RESPONSE_TYPE,
  Message: Message,
  Protocol: Protocol,
  Registry: Registry,
  discoverProtocolSchema: discoverProtocolSchema,
  streams: {
    FrameDecoder: FrameDecoder,
    FrameEncoder: FrameEncoder,
    NettyDecoder: NettyDecoder,
    NettyEncoder: NettyEncoder
  }
};
