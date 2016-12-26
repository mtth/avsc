/* jshint node: true */

// TODO: Explore making `MessageEmitter` a writable stream, and
// `MessageListener` a readable stream. The main inconsistency is w.r.t.
// watermarks (the standard stream behavior doesn't support waiting for the
// callbacks, without also preventing concurrent requests).
// TODO: Add broadcast option to client `_emitMessage`, accessible for one-way
// message.
// TODO: Add `server.mount` method to allow combining servers. The API is as
// follows: a mounted server's (i.e. the method's argument) handlers have lower
// precedence than the original server (i.e. `this`); the mounted server's
// middlewares are only invoked for its handlers.

'use strict';

/**
 * This module implements Avro's IPC/RPC logic.
 *
 * This is done the Node.js way, mimicking the `EventEmitter` class.
 */

var types = require('./types'),
    utils = require('./utils'),
    events = require('events'),
    stream = require('stream'),
    util = require('util');


// A few convenience imports.
var Tap = utils.Tap;
var Type = types.Type;
var debug = util.debuglog('avsc:services');
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

// Internal message, used to check protocol compatibility.
var PING_MESSAGE = new Message(
  '',
  Type.forSchema({name: 'PingRequest', type: 'record', fields: []}, OPTS),
  Type.forSchema(['string'], OPTS),
  Type.forSchema('null', OPTS)
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
  this._oneWay = !!isOneWay;
  this._doc = doc !== undefined ? '' + doc : undefined;
}

Message.forSchema = function (name, schema, opts) {
  opts = opts || {};
  if (!types.isValidName(name)) {
    throw new Error(f('invalid message name: %s', name));
  }

  // We use a record with a placeholder name here (the user might have set
  // `noAnonymousTypes`, so we can't use an anonymous one). We remove it from
  // the registry afterwards to avoid exposing it outside.
  var recordName = f('%s.%sRequest', OPTS.namespace, utils.capitalize(name));
  var reqType = Type.forSchema({
    name: recordName,
    type: 'record',
    namespace: opts.namespace || '', // Don't leak request namespace.
    fields: schema.request
  }, opts);
  delete opts.registry[recordName];

  if (!schema.response) {
    throw new Error('missing response');
  }
  var resType = Type.forSchema(schema.response, opts);
  var errType = Type.forSchema(['string'].concat(schema.errors || []), opts);
  var isOneWay = !!schema['one-way'];
  return new Message(name, reqType, errType, resType, isOneWay, schema.doc);
};

Message.prototype.getDocumentation = function () { return this._doc; };

Message.prototype.getErrorType = function () { return this._errType; };

Message.prototype.getName = function () { return this._name; };

Message.prototype.getRequestType = function () { return this._reqType; };

Message.prototype.getResponseType = function () { return this._resType; };

Message.prototype.isOneWay = function () { return this._oneWay; };

// Deprecated (used only internally by `service.getSchema`).

Message.prototype._attrs = function (opts) {
  var reqSchema = this.getRequestType()._attrs(opts);
  var schema = {
    request: reqSchema.fields,
    response: this.getResponseType()._attrs(opts)
  };
  var msgDoc = this.getDocumentation();
  if (msgDoc !== undefined) {
    schema.doc = msgDoc;
  }
  var errSchema = this.getErrorType()._attrs(opts);
  if (errSchema.length > 1) {
    schema.errors = errSchema.slice(1);
  }
  if (this.isOneWay()) {
    schema['one-way'] = true;
  }
  return schema;
};

/**
 * An Avro RPC service.
 *
 * This constructor shouldn't be called directly, but via the
 * `Service.forProtocol` method. This function performs little logic to better
 * support efficient copy.
 */
function Service(name, messages, types, ptcl, server) {
  if (typeof name != 'string') {
    // Let's be helpful in case this class is instantiated directly.
    return Service.forProtocol(name, messages);
  }

  this._name = name;
  this._messagesByName = messages || {};
  this._messages = Object.freeze(utils.objectValues(this._messagesByName));

  this._typesByName = types || {};
  this._types = utils.objectValues(this._typesByName).filter(function (type) {
    return type.getName() !== undefined;
  });
  Object.freeze(this._types);

  // We cache a string rather than a buffer to not retain an entire slab.
  this._ptclStr = JSON.stringify(ptcl);
  this._hashStr = utils.getHash(this._ptclStr).toString('binary');
  this._doc = ptcl.doc ? '' + ptcl.doc : undefined;

  // Deprecated fields.

  // Hash string, used by `getFingerprint`.
  this._hs = undefined;

  // We add a server to each protocol for backwards-compatibility (to allow the
  // use of `protocol.on`). This covers all cases except the use of the
  // `strictErrors` option, which requires moving to the new API.
  this._server = server || this.createServer({silent: true});
}

Service.forProtocol = function (ptcl, opts) {
  opts = opts || {};

  var name = ptcl.protocol;
  if (!name) {
    throw new Error('missing protocol name');
  }
  if (ptcl.namespace !== undefined) {
    opts.namespace = ptcl.namespace;
  } else {
    var match = /^(.*)\.[^.]+$/.exec(name);
    if (match) {
      opts.namespace = match[1];
    }
  }
  name = types.qualify(name, opts.namespace);

  if (ptcl.types) {
    ptcl.types.forEach(function (obj) { Type.forSchema(obj, opts); });
  }
  var msgs;
  if (ptcl.messages) {
    msgs = {};
    Object.keys(ptcl.messages).forEach(function (key) {
      msgs[key] = Message.forSchema(key, ptcl.messages[key], opts);
    });
  }

  return new Service(name, msgs, opts.registry, ptcl);
};

Service.isService = function (any) {
  // Not fool-proof but likely sufficient.
  return (
    !!any &&
    any.hasOwnProperty('_hashStr') &&
    typeof any.getHash == 'function'
  );
};

Service.prototype.createClient = function (opts) {
  var client = new Client(this, opts);
  if (opts && opts.server) {
    // Convenience in-memory client. This can be useful to make requests
    // relatively efficiently to an in-process server. Note that it is still is
    // less efficient than direct method calls (because of the serialization,
    // which does provide "type-safety" though).
    var obj = {objectMode: true};
    var pts = [new stream.PassThrough(obj), new stream.PassThrough(obj)];
    opts.server.createListener({readable: pts[0], writable: pts[1]}, obj);
    client.createEmitter({readable: pts[1], writable: pts[0]}, obj);
  } else if (opts && opts.transport) {
    // Convenience functionality for the common single emitter use-case: we
    // add a single emitter using default options to the client.
    client.createEmitter(opts.transport);
  }
  return client;
};

Service.prototype.createServer = function (opts) {
  return new Server(this, opts);
};

Service.prototype.getDocumentation = function () { return this._doc; };

Service.prototype.getHash = function () {
  return new Buffer(this._hashStr, 'binary');
};

Service.prototype.getMessage = function (name) {
  return this._messagesByName[name];
};

Service.prototype.getMessages = function () { return this._messages; };

Service.prototype.getName = function () { return this._name; };

Service.prototype.getProtocol = function () {
  return JSON.parse(this._ptclStr);
};

Service.prototype.getType = function (name) {
  return this._typesByName[name];
};

Service.prototype.getTypes = function () { return this._types; };

Service.prototype.inspect = function () {
  return f('<Service %j>', this._name);
};

// Deprecated methods.

Service.prototype.createEmitter = util.deprecate(
  function (transport, opts) {
    var client = this.createClient(opts);
    var emitter = client.createEmitter(transport, opts);
    client.on('error', function (err) { emitter.emit('error', err); });
    return emitter;
  },
  'use `createClient(/* ... */)` instead'
);

Service.prototype.createListener = util.deprecate(
  function (transport, opts) {
    if (opts && opts.strictErrors) {
      throw new Error('use `protocol.createServer` to support strict errors');
    }
    return this._server.createListener(transport, opts);
  },
  'use `createServer(/* ... */).createListener(/* ... */)` instead'
);

Service.prototype.emit = util.deprecate(
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

Service.prototype.equals = util.deprecate(
  function (any) {
    return (
      Service.isService(any) &&
      this.getFingerprint().equals(any.getFingerprint())
    );
  },
  'service equality testing will be removed in a future version'
);

Service.prototype.getFingerprint = util.deprecate(
  Type.prototype.getFingerprint,
  'use `getHash()` instead'
);

Service.prototype.getHandler = util.deprecate(
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

Service.prototype.getSchema = util.deprecate(
  Type.prototype.getSchema,
  'use `getProtocol()` instead'
);

Service.prototype.on = util.deprecate(
  function (name, handler) {
    var self = this; // This protocol.
    this._server.onMessage(name, function (req, cb) {
      return handler.call(self, req, this, cb);
    });
    return this;
  },
  'use `createServer().onMessage(/* ... */)` instead'
);

Service.prototype.subprotocol = util.deprecate(
  function () {
    var parent = this._server;
    var opts = {strictErrors: parent._strict, cache: parent._cache};
    var server = new Server(parent._svc, opts);
    server._handlers = Object.create(parent._handlers);
    return new Service(
      this._name,
      this._messagesByName,
      this._typesByName,
      this.getProtocol(),
      server
    );
  },
  'subprotocol support will be removed in a future version'
);

Service.prototype._attrs = function (opts) {
  var ptcl = {protocol: this._name};

  if (this._types.length) {
    ptcl.types = [];
    this._types.forEach(function (t) {
      var typeSchema = t._attrs(opts);
      if (typeof typeSchema != 'string') {
        // Some of the named types might already have been defined in a
        // previous type, in this case we don't include its reference.
        ptcl.types.push(typeSchema);
      }
    });
  }

  var msgNames = Object.keys(this._messagesByName);
  if (msgNames.length) {
    ptcl.messages = {};
    msgNames.forEach(function (name) {
      ptcl.messages[name] = this._messagesByName[name]._attrs(opts);
    }, this);
  }

  if (opts && opts.exportAttrs && this._doc !== undefined) {
    ptcl.doc = this._doc;
  }
  return ptcl;
};

/** Function to retrieve a remote service's protocol. */
function discoverProtocol(transport, opts, cb) {
  if (cb === undefined && typeof opts == 'function') {
    cb = opts;
    opts = undefined;
  }

  var svc = new Service({protocol: 'Empty'}, OPTS);
  var client = svc.createClient({defaultTimeout: opts && opts.timeout});
  var emitter = client.createEmitter(transport, {
    scope: opts && opts.scope,
    endWritable: typeof transport == 'function' // Stateless transports only.
  });
  emitter
    .once('handshake', function (hreq, hres) {
      this.destroy(true); // Prevent further requests on this emitter.
      cb(null, JSON.parse(hres.serverProtocol));
    })
    .once('error', function (err) {
      // Stateless transports will throw an interrupted error when the emitter
      // is destroyed, we ignore it here.
      this.destroy(true);
      if (!/interrupted/.test(err)) {
        cb(err); // Likely timeout.
      }
    });
}

/** Load-balanced message sender. */
function Client(svc, opts) {
  opts = opts || {};
  events.EventEmitter.call(this);

  // We have to suffix all client properties to be safe, since the message
  // names aren't prefixed with clients (unlike servers).
  this._svc$ = svc;
  this._emitters$ = []; // Active emitters.
  this._fns$ = []; // Middleware functions.

  this._cache$ = opts.cache || {}; // For backwards compatibility.
  this._policy$ = opts.emitterPolicy;
  this._strict$ = !!opts.strictErrors;
  this._timeout$ = utils.getOption(opts, 'defaultTimeout', 10000);

  if (opts.remoteProtocols) {
    Adapter._populateCache(this._cache$, svc, opts.remoteProtocols, true);
  }

  this._svc$.getMessages().forEach(function (msg) {
    this[msg.getName()] = this._createMessageHandler(msg);
  }, this);
}
util.inherits(Client, events.EventEmitter);

Client.prototype.createEmitter = function (transport, opts) {
  var objectMode = opts && opts.objectMode;
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
        var encoder = new FrameEncoder();
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
    // Remove the emitter from the list of active ones.
    emitters.splice(emitters.indexOf(this), 1);
  });
  return emitter;
};

Client.prototype.destroyEmitters = function (opts) {
  this._emitters$.forEach(function (emitter) {
    emitter.destroy(opts && opts.noWait);
  });
};

Client.prototype.emitMessage = function (name, req, opts, cb) {
  if (!cb && typeof opts === 'function') {
    cb = opts;
    opts = undefined;
  }
  var msg = getExistingMessage(this._svc$, name);
  var wreq = new WrappedRequest(msg, {}, req);
  return Client.prototype._emitMessage.call(this, wreq, opts, cb);
};

Client.prototype.getEmitters = function () {
  return Object.freeze(this._emitters$.slice());
};

Client.prototype.getRemoteProtocols = function () {
  return Adapter._getRemoteProtocols(this._svc$, this._cache$, true);
};

Client.prototype.getService = function () { return this._svc$; };

Client.prototype.use = function (/* fn ... */) {
  var i, l;
  for (i = 0, l = arguments.length; i < l; i++) {
    this._fns$.push(arguments[i]);
  }
  return this;
};

Client.prototype._emitMessage = function (wreq, opts, cb) {
  // Common logic between `client.emitMessage` and the "named" message methods.
  opts = opts || {};
  if (opts.timeout === undefined) {
    opts.timeout = this._timeout$;
  }

  // Select which emitter to use.
  var emitters = this._emitters$;
  var numEmitters = emitters.length;
  if (!numEmitters) {
    var err = new Error('no emitters available');
    if (cb) {
      cb(err);
    } else {
      this.emit('error', err);
    }
    return;
  }
  var self = this;
  var emitter;
  if (numEmitters === 1) {
    // Common case, optimized away.
    emitter = emitters[0];
  } else if (this._policy$) {
    emitter = this._policy$(this._emitters$.slice());
  } else {
    // Random selection, cheap and likely good enough for most use-cases.
    emitter = emitters[Math.floor(Math.random() * numEmitters)];
  }

  return emitter._emit(wreq, opts, function (err, wres) {
    var ctx = this; // Call context.
    var errType = wreq.getMessage().getErrorType();
    if (err) {
      // System error, likely the message wasn't sent (or an error occurred
      // while decoding the response).
      if (self._strict$) {
        var errStr = err.stack || /* istanbul ignore next */ err.toString();
        err = errType.clone(errStr, {wrapUnions: true});
      }
      done(err);
      return;
    }
    if (!wres) {
      // This is a one way message.
      done();
      return;
    }
    // Message transmission succeeded, we transmit the message data; massaging
    // any error strings into actual `Error` objects in non-strict mode.
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

    function done(err, res) {
      if (cb) {
        cb.call(ctx, err, res);
      } else if (err) {
        emitter.emit('error', err);
      }
    }
  });
};

Client.prototype._createMessageHandler = function (msg) {
  // jshint -W054
  var fields = msg.getRequestType().getFields();
  var names = fields.map(function (f) { return f.getName(); });
  var body = 'return function ' + msg.getName() + '(';
  if (names.length) {
    body += names.join(', ') + ', ';
  }
  body += 'opts, cb) {\n';
  body += '  if (!cb && typeof opts == \'function\') {\n';
  body += '    cb = opts;\n';
  body += '    opts = undefined;\n';
  body += '  }\n';
  body += '  var req = {\n    ';
  body += names.map(function (n) { return n + ': ' + n; }).join(',\n    ');
  body += '\n  };\n';
  body += '  var wreq = new WrappedRequest(msg, {}, req);\n';
  body += '  return emit.call(this, wreq, opts, cb);\n';
  body += '};';
  var fn = new Function('WrappedRequest,msg,emit', body);
  return fn(WrappedRequest, msg, Client.prototype._emitMessage);
};

/** Message receiver. */
function Server(svc, opts) {
  opts = opts || {};
  events.EventEmitter.call(this);

  this._svc = svc;
  this._handlers = {};
  this._fns = []; // Middleware functions.
  this._listeners = {}; // Active listeners.
  this._nextListenerId = 1;

  this._cache = opts.cache || {}; // Deprecated.
  this._errorFormatter = opts.errorFormatter;
  this._silent = !!opts.silent;
  this._strict = !!opts.strictErrors;

  if (opts.remoteProtocols) {
    Adapter._populateCache(this._cache, svc, opts.remoteProtocols, false);
  }

  svc.getMessages().forEach(function (msg) {
    var name = msg.getName();
    if (!opts.noCapitalize) {
      name = utils.capitalize(name);
    }
    this['on' + name] = this._createMessageHandler(msg);
  }, this);
}
util.inherits(Server, events.EventEmitter);

Server.prototype.createListener = function (transport, opts) {
  var objectMode = opts && opts.objectMode;
  var listener;
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
    if (!objectMode) {
      var decoder = new NettyDecoder();
      readable = readable.pipe(decoder);
      var encoder = new NettyEncoder();
      encoder.pipe(writable);
      writable = encoder;
    }
    listener = new StatefulListener(this, readable, writable, opts);
    if (!objectMode) {
      // Similar to emitters, since we never expose the encoder and decoder, we
      // must release them ourselves here.
      listener.once('eot', function () {
        readable.unpipe(decoder);
        encoder.unpipe(writable);
      });
    }
  }

  if (!this.listeners('error').length) {
    this.on('error', this._onError);
  }
  var listenerId = this._nextListenerId++;
  var listeners = this._listeners;
  listeners[listenerId] = listener;
  listener.once('eot', function () { delete listeners[listenerId]; });
  this.emit('listener', listener);
  return listener;
};

Server.prototype.getListeners = function () {
  return Object.freeze(utils.objectValues(this._listeners));
};

Server.prototype.getRemoteProtocols = function () {
  return Adapter._getRemoteProtocols(this._svc, this._cache, false);
};

Server.prototype.getService = function () { return this._svc; };

Server.prototype.onMessage = function (name, handler) {
  var msg = getExistingMessage(this.getService(), name);
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

Server.prototype.use = function (/* fn ... */) {
  var i, l;
  for (i = 0, l = arguments.length; i < l; i++) {
    this._fns.push(arguments[i]);
  }
  return this;
};

Server.prototype._createMessageHandler = function (msg) {
  // jshint -W054
  var body = 'return function (handler) {\n';
  body += '  return server.onMessage(\'' + msg.getName() + '\', ';
  body += 'function (req, cb) {\n';
  body += '    handler.call(this';
  var fields = msg.getRequestType().getFields();
  if (fields.length) {
    var args = fields.map(function (f) { return 'req.' + f.getName(); });
    body += ', ' + args.join(', ');
  }
  body += ', cb);\n';
  body += '  });\n';
  body += '};\n';
  return (new Function('server', body))(this);
};

Server.prototype._onError = function (err) {
  /* istanbul ignore if */
  if (!this._silent && err.rpcCode !== 'UNKNOWN_PROTOCOL') {
    console.error();
    console.error(err.rpcCode || 'INTERNAL_SERVER_ERROR');
    console.error(err.stack || err.toString());
  }
};

/** Base message emitter class. See below for the two available variants. */
function MessageEmitter(client, opts) {
  opts = opts || {};
  events.EventEmitter.call(this);

  this._client = client;
  this._endWritable = !!utils.getOption(opts, 'endWritable', true);
  this._timeout = utils.getOption(opts, 'timeout', client._timeout$);
  this._prefix = normalizedPrefix(opts.scope);
  this._callContext = opts.callContext;

  var cache = client._cache$;
  var clientSvc = client._svc$;
  var hash = opts.serverHash;
  if (!hash) {
    hash = clientSvc.getHash();
  }
  var adapter = cache[hash];
  if (!adapter) {
    // This might happen even if the server fingerprint option was set if the
    // cache doesn't contain the corresponding adapter. In this case we fall
    // back to the client's protocol (as mandated by the spec).
    hash = clientSvc.getHash();
    adapter = cache[hash] = new Adapter(clientSvc, clientSvc, hash);
  }
  this._adapter = adapter;

  this._registry = new Registry(this, PREFIX_LENGTH);
  this._pending = 0;
  this._destroyed = false;
  this._interrupted = false;
  this.once('_eot', function (pending) {
    var self = this;
    process.nextTick(function () {
      debug('emitter EOT');
      self.emit('eot', pending);
    });
  });
}
util.inherits(MessageEmitter, events.EventEmitter);

MessageEmitter.prototype.destroy = function (noWait) {
  debug('destroying emitter');
  this._destroyed = true;
  var registry = this._registry;
  var pending = this._pending;
  if (noWait && pending) {
    this._interrupted = true;
    registry.clear();
  }
  if (noWait || !pending) {
    this.emit('_eot', pending);
  }
};

MessageEmitter.prototype.getClient = function () { return this._client; };

MessageEmitter.prototype.getPending = function () { return this._pending; };

MessageEmitter.prototype.isDestroyed = function () { return this._destroyed; };

MessageEmitter.prototype._createHandshakeRequest = function (adapter, noSvc) {
  var svc = this._client._svc$;
  return {
    clientHash: svc.getHash(),
    clientProtocol: noSvc ? null : svc._ptclStr,
    serverHash: adapter._fingerprint
  };
};

MessageEmitter.prototype._emit = function (wreq, opts, cb) {
  if (cb === undefined && typeof opts == 'function') {
    cb = opts;
    opts = undefined;
  }

  var msg = wreq.getMessage();
  var wres = msg.isOneWay() ? undefined : new WrappedResponse(msg, {});
  var ctx = expandOption(this._callContext, [this, opts]);
  var self = this;
  this._pending++;
  process.nextTick(function () {
    if (!wreq.getMessage().getName()) {
      // Ping request, bypass middleware.
      onTransition(wreq, wres, onCompletion);
    } else {
      var fns = self._client._fns$;
      debug('entering emitter forward phase (%s middleware)', fns.length );
      chainFunctions(
        fns,
        ctx,
        [wreq, wres],
        onTransition,
        onCompletion,
        onError
      );
    }
  });
  return this._pending;

  function onTransition(wreq, wres, prev) {
    // Serialize the message.
    var err, reqBuf;
    if (self._destroyed) {
      err = new Error('destroyed');
    } else {
      try {
        reqBuf = wreq.toBuffer();
      } catch (cause) {
        err = toRpcError('INVALID_REQUEST', cause);
      }
    }
    if (err) {
      prev(err);
      return;
    }

    // Generate the response callback.
    var msg = wreq.getMessage();
    var timeout = (opts && opts.timeout !== undefined) ?
      opts.timeout :
      self._timeout;
    var id = self._registry.add(timeout, function (err, resBuf, adapter) {
      if (!err && !msg.isOneWay()) {
        try {
          adapter._decodeResponse(resBuf, wres);
        } catch (cause) {
          err = toRpcError('INVALID_RESPONSE', cause);
        }
      }
      prev(err);
    });
    id |= self._prefix;

    debug('sending message %s', id);
    self._send(id, reqBuf, !!msg && msg.isOneWay());
  }

  function onCompletion(err) {
    self._pending--;
    if (self._destroyed && !self._interrupted && !self._pending) {
      self.destroy();
    }
    cb.call(ctx, err, wres);
  }

  function onError(err) {
    self.emit('error', err);
  }
};

MessageEmitter.prototype._getAdapter = function (hres) {
  var hash = hres.serverHash;
  var cache = this._client._cache$;
  var adapter = cache[hash];
  if (adapter) {
    return adapter;
  }
  var ptcl = JSON.parse(hres.serverProtocol);
  // Wrapping is required to support all schemas, but has no effect on the
  // final output (controlled by the server's protocol) since resolution is
  // independent of whether unions are wrapped or not.
  var serverSvc = Service.forProtocol(ptcl, {wrapUnions: true});
  adapter = new Adapter(this._client._svc$, serverSvc, hash, true);
  return cache[hash] = adapter;
};

MessageEmitter.prototype._matchesPrefix = function (id) {
  return matchesPrefix(id, this._prefix);
};

MessageEmitter.prototype._send = utils.abstractFunction;

// Deprecated.

MessageEmitter.prototype.emitMessage = util.deprecate(
  function (name, reqEnv, opts, cb) {
    if (cb === undefined && typeof opts == 'function') {
      cb = opts;
      opts = undefined;
    }
    if (!cb) {
      throw new Error('missing callback');
    }

    var svc = this._client.getService();
    var msg = svc.getMessage(name);
    if (!msg) {
      // Deprecated.
      process.nextTick(function () {
        cb.call(svc, new Error(f('unknown message: %s', name)));
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
  'use `Client.emitMessage(/* ... */)` instead'
);

MessageEmitter.prototype.getCache = util.deprecate(
  function () { return this._client._cache$; },
  'use `getClient().getRemoteProtocols()` instead'
);

MessageEmitter.prototype.getProtocol = util.deprecate(
  function () {
    return Client.prototype.getService.call(this._client);
  },
  'use `getClient().getService()` instead'
);

MessageEmitter.prototype.getTimeout = util.deprecate(
  function () { return this._timeout; },
  'use client default timeout instead'
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
    debug('emitting ping request');
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
        } catch (cause) {
          cb(toRpcError('INVALID_HANDSHAKE_RESPONSE', cause));
          return;
        }
        debug('handshake match: %s', hres.match);
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
    } catch (cause) {
      self.destroy(true); // Not a recoverable error.
      var err = toRpcError('INVALID_HANDSHAKE_RESPONSE', cause);
      self._client.emit('error', err);
      return;
    }
    debug('handshake match: %s', hres.match);
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
  this._writable.write({id: id, payload: [reqBuf]});
};

/** The server-side emitter equivalent. */
function MessageListener(server, opts) {
  opts = opts || {};
  events.EventEmitter.call(this);

  this._server = server;
  this._endWritable = !!utils.getOption(opts, 'endWritable', true);
  this._prefix = normalizedPrefix(opts.scope);
  this._callContext = opts.callContext;
  this._errorFormatter = opts.errorFormatter;

  var cache = server._cache;
  var svc = server.getService();
  var hash = svc.getHash();
  if (!cache[hash]) {
    // Add the listener's protocol to the cache if it isn't already there. This
    // will save a handshake the first time on emitters with the same protocol.
    cache[hash] = new Adapter(svc, svc, hash);
  }

  this._adapter = null;
  this._hook = null;

  this._pending = 0;
  this._destroyed = false;
  this._interrupted = false;
  this.once('_eot', function (pending) {
    var self = this;
    process.nextTick(function () { self.emit('eot', pending); });
  });
}
util.inherits(MessageListener, events.EventEmitter);

MessageListener.prototype.destroy = function (noWait) {
  this._destroyed = true;
  if (noWait || !this._pending) {
    this._interrupted = true;
    this.emit('_eot', this._pending);
  }
};

MessageListener.prototype.getPending = function () { return this._pending; };

MessageListener.prototype.getServer = function () { return this._server; };

MessageListener.prototype.isDestroyed = function () {
  return this._destroyed;
};

MessageListener.prototype._createHandshakeResponse = function (err, hreq) {
  var svc = this._server.getService();
  var buf = svc.getHash();
  var serverMatch = hreq && hreq.serverHash.equals(buf);
  return {
    match: err ? 'NONE' : (serverMatch ? 'BOTH' : 'CLIENT'),
    serverProtocol: serverMatch ? null : svc._ptclStr,
    serverHash: serverMatch ? null : buf
  };
};

MessageListener.prototype._getAdapter = function (hreq) {
  var hash = hreq.clientHash;
  var adapter = this._server._cache[hash];
  if (adapter) {
    return adapter;
  }
  if (!hreq.clientProtocol) {
    throw toRpcError('UNKNOWN_PROTOCOL');
  }
  var ptcl = JSON.parse(hreq.clientProtocol);
  // See `MessageEmitter._getAdapter` for `wrapUnions` motivation.
  var clientSvc = Service.forProtocol(ptcl, {wrapUnions: true});
  adapter = new Adapter(clientSvc, this._server.getService(), hash, true);
  return this._server._cache[hash] = adapter;
};

MessageListener.prototype._matchesPrefix = function (id) {
  return matchesPrefix(id, this._prefix);
};

MessageListener.prototype._receive = function (reqBuf, adapter, cb) {
  var self = this;
  var wreq;
  try {
    wreq = adapter._decodeRequest(reqBuf);
  } catch (err) {
    cb(self._encodeError(err));
    return;
  }

  var msg = wreq.getMessage();
  var wres = msg.isOneWay() ? undefined : new WrappedResponse(msg, {});
  if (!msg.getName()) {
    // Ping message, we don't invoke middleware logic in this case.
    wres.setResponse(null);
    cb(wres.toBuffer(), false);
    return;
  }

  var ctx = expandOption(this._callContext, [this]);
  self._pending++;
  chainFunctions(
    this._server._fns,
    ctx,
    [wreq, wres],
    onTransition,
    onCompletion,
    onError
  );

  function onTransition(wreq, wres, prev) {
    /* istanbul ignore if */
    if (self._hook) {
      // Legacy hook.
      var eq = {header: wreq.getHeader(), request: wreq.getRequest()};
      self._hook.call(self, msg.getName(), eq, function (err, es) {
        if (wres) {
          utils.copyOwnProperties(es.header, wres.getHeader());
        }
        if (err) {
          prev(err);
          return;
        }
        if (es.error) {
          wres.setError(es.error);
        } else {
          wres.setResponse(es.response);
        }
        prev(null);
      });
    } else {
      var handler = self._server._handlers[msg.getName()];
      if (!handler) {
        // The underlying protocol hasn't implemented a handler.
        prev(toRpcError('NOT_IMPLEMENTED'));
      } else if (msg.isOneWay()) {
        handler.call(ctx, wreq.getRequest());
        prev(null);
      } else {
        try {
          handler.call(ctx, wreq.getRequest(), function (err, res) {
            if (err) {
              wres.setError(err);
            } else {
              wres.setResponse(res);
            }
            prev(null);
          });
        } catch (err) {
          // We catch synchronous failures (same as express) and return the
          // failure. Note that the server process can still crash if an error
          // is thrown after the handler returns but before the response is
          // sent (again, same as express). We don't do this for one-way
          // messages because potential errors would then easily pass
          // unnoticed.
          prev(err);
        }
      }
    }
  }

  function onCompletion(err) {
    self._pending--;
    var resBuf;
    if (!err) {
      try {
        resBuf = wres.toBuffer();
      } catch (cause) {
        err = cause;
      }
    }
    if (err) {
      resBuf = self._encodeError(err, wres ? wres.getHeader() : undefined);
    }
    if (!self._interrupted) {
      cb(resBuf, msg.isOneWay());
    }
    if (self._destroyed && !self._pending) {
      self.destroy();
    }
  }

  function onError(err) {
    self.emit('error', err);
  }
};

// Deprecated.

MessageListener.prototype.getCache = util.deprecate(
  function () { return this._server._cache; },
  'use `getServer().getRemoteProtocols()` instead'
);

MessageListener.prototype.getProtocol = util.deprecate(
  function () {
    return this._server.getService();
  },
  'use `getServer().getService()` instead'
);

MessageListener.prototype.onMessage = util.deprecate(
  function (fn) {
    this._hook = fn;
    return this;
  },
  'use `getServer().use(/* ... */)` to implement middleware instead'
);

/**
 * Encode an error and optional header into a valid Avro response.
 *
 * @param err {Error} Error to encode.
 * @param header {Object} Optional response header.
 */
MessageListener.prototype._encodeError = function (err, header) {
  var server = this._server;
  server.emit('error', err);
  var errStr;
  if (server._errorFormatter) {
    // Format the error into a string to send over the wire.
    try {
      errStr = server._errorFormatter.call(this, err);
    } catch (cause) {
      server.emit('error', cause);
    }
  }
  var hdrBuf;
  if (header) {
    try {
      // Propagate the header if possible.
      hdrBuf = MAP_BYTES_TYPE.toBuffer(header);
    } catch (cause) {
      server.emit('error', cause);
    }
  }
  return Buffer.concat([
    hdrBuf || new Buffer([0]),
    new Buffer([1, 0]), // Error flag and first union index.
    STRING_TYPE.toBuffer(errStr || err.rpcCode || 'INTERNAL_SERVER_ERROR')
  ]);
};

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
    try {
      var parts = readHead(HANDSHAKE_REQUEST_TYPE, buf);
      var hreq = parts.head;
      var adapter = self._getAdapter(hreq);
    } catch (cause) {
      var err = toRpcError('INVALID_HANDSHAKE_REQUEST', cause);
      done(self._encodeError(err));
      return;
    }

    self._receive(parts.tail, adapter, done);

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
      err = toRpcError('INVALID_HANDSHAKE_REQUEST', cause);
    }
    if (err) {
      // Either the client's protocol was unknown or it isn't compatible.
      done(self._encodeError(err));
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

/** Enhanced request, used inside forward middleware functions. */
function WrappedRequest(msg, hdr, req) {
  this._msg = msg;
  this._hdr = hdr || {};
  this._req = req || {};
}

WrappedRequest.prototype.getHeader = function () { return this._hdr; };

WrappedRequest.prototype.getMessage = function () { return this._msg; };

WrappedRequest.prototype.getRequest = function () { return this._req; };

WrappedRequest.prototype.toBuffer = function () {
  var msg = this._msg;
  return Buffer.concat([
    MAP_BYTES_TYPE.toBuffer(this._hdr),
    STRING_TYPE.toBuffer(msg.getName()),
    msg.getRequestType().toBuffer(this._req)
  ]);
};

/** Enhanced response, used inside forward middleware functions. */
function WrappedResponse(msg, hdr, err, res) {
  this._msg = msg;
  this._hdr = hdr;
  this._err = err;
  this._res = res;
}

WrappedResponse.prototype.hasError = function () {
  return this._err !== undefined;
};

WrappedResponse.prototype.getError = function () { return this._err; };

WrappedResponse.prototype.getHeader = function () { return this._hdr; };

WrappedResponse.prototype.getMessage = function () { return this._msg; };

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
 * Callback registry.
 *
 * Callbacks added must accept an error as first argument. This is used by
 * message emitters to store pending calls. This class isn't exposed by the
 * public API.
 */
function Registry(ctx, prefixLength) {
  this._ctx = ctx; // Context for all callbacks.
  this._mask = ~0 >>> (prefixLength | 0); // 16 bits by default.
  this._id = 0; // Unique integer ID for each call.
  this._n = 0; // Number of pending calls.
  this._cbs = {};
}

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
 * Service resolution helper.
 *
 * It is used both by emitters and listeners, to respectively decode errors and
 * responses, or requests.
 */
function Adapter(clientSvc, serverSvc, fingerprint, isRemote) {
  this._clientSvc = clientSvc;
  this._serverSvc = serverSvc;
  this._fingerprint = fingerprint; // Convenience.
  this._isRemote = !!isRemote;
  this._readers = this._createReaders();
}

Adapter._populateCache = function (cache, svc, ptcls, isClient) {
  Object.keys(ptcls).forEach(function (hash) {
    var ptcl = ptcls[hash];
    var clientSvc, serverSvc;
    if (isClient) {
      clientSvc = svc;
      serverSvc = Service.forProtocol(ptcl, {wrapUnions: true});
    } else {
      clientSvc = Service.forProtocol(ptcl, {wrapUnions: true});
      serverSvc = svc;
    }
    cache[hash] = new Adapter(clientSvc, serverSvc, hash, true);
  });
  return cache;
};

Adapter._getRemoteProtocols = function (svc, cache, isClient) {
  var ptcls = {};
  Object.keys(cache).forEach(function (hs) {
    var adapter = cache[hs];
    if (adapter._isRemote) {
      var svc = isClient ? adapter._serverSvc : adapter._clientSvc;
      ptcls[hs] = JSON.parse(svc._ptclStr);
    }
  });
  return Object.freeze(ptcls);
};

Adapter.prototype._createReaders = function () {
  var obj = {};
  this._clientSvc.getMessages().forEach(function (c) {
    var n = c.getName();
    var s = this._serverSvc.getMessage(n);
    if (!s) {
      throw new Error(f('missing server message: %s', n));
    }
    if (s.isOneWay() !== c.isOneWay()) {
      throw new Error(f('inconsistent one-way parameter for message: %s', n));
    }
    try {
      obj[n + '?'] = createReader(s.getRequestType(), c.getRequestType());
      obj[n + '*'] = createReader(c.getErrorType(), s.getErrorType());
      obj[n + '!'] = createReader(c.getResponseType(), s.getResponseType());
    } catch (cause) {
      throw toRpcError('INCOMPATIBLE_PROTOCOL', cause);
    }
  }, this);
  return obj;
};

Adapter.prototype._decodeRequest = function (buf) {
  var tap = new Tap(buf);
  var hdr = MAP_BYTES_TYPE._read(tap);
  var name = STRING_TYPE._read(tap);
  var msg, req;
  if (name) {
    msg = this._serverSvc.getMessage(name);
    req = this._readers[name + '?']._read(tap);
  } else {
    msg = PING_MESSAGE;
  }
  if (!tap.isValid()) {
    throw new Error('truncated request');
  }
  return new WrappedRequest(msg, hdr, req);
};

Adapter.prototype._decodeResponse = function (buf, wres) {
  var tap = new Tap(buf);
  utils.copyOwnProperties(MAP_BYTES_TYPE._read(tap), wres.getHeader(), true);
  var isError = BOOLEAN_TYPE._read(tap);
  var name = wres.getMessage().getName();
  var msg;
  if (name) {
    var reader = this._readers[name + (isError ? '*' : '!')];
    msg = this._clientSvc.getMessage(name);
    if (isError) {
      wres.setError(reader._read(tap));
    } else {
      wres.setResponse(reader._read(tap));
    }
  } else {
    msg = PING_MESSAGE;
  }
  if (!tap.isValid()) {
    throw new Error('truncated response');
  }
};

// Deprecated (eventually, once the deprecated `emitter.getCache` and
// `listener.getCache()` methods have been removed, this class won't be exposed
// at all via the public API).

Adapter.prototype.getClientProtocol = util.deprecate(
  function () { return this._clientSvc; },
  'use `server.getRemoteProtocols()` instead.'
);

Adapter.prototype.getServerProtocol = util.deprecate(
  function () { return this._serverSvc; },
  'use `client.getRemoteProtocols()` instead.'
);

/** Standard "un-framing" stream. */
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

/** Standard framing stream. */
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

/** Netty-compatible decoding stream. */
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

/** Netty-compatible encoding stream. */
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
 * Generate a decoder, optimizing the case where reader and writer are equal.
 *
 * @param rtype {Type} Reader's type.
 * @param wtype {Type} Writer's type.
 */
function createReader(rtype, wtype) {
  return rtype.equals(wtype) ? rtype : rtype.createResolver(wtype);
}

/**
 * Check whether something is an `Error`.
 *
 * @param any {Object} Any object.
 */
function isError(any) {
  // Also not ideal, but avoids brittle `instanceof` checks.
  return !!any && Object.prototype.toString.call(any) === '[object Error]';
}

/**
 * Mark an error.
 *
 * @param rpcCode {String} Code representing the failure.
 * @param cause {Error} The cause of the error. It is available as `cause`
 * field on the outer error.
 *
 * This is used to keep the argument of emitters' `'error'` event errors.
 */
function toRpcError(rpcCode, err) {
  if (!err) {
    err = new Error();
  }
  if (!err.rpcCode) {
    err.rpcCode = rpcCode;
  }
  return err;
}

/**
 * Compute a prefix of fixed length from a string.
 *
 * @param scope {String} Namespace to be hashed.
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
 */
function matchesPrefix(id, prefix) {
  return ((id ^ prefix) >> (32 - PREFIX_LENGTH)) === 0;
}

/**
 * Check whether something is a stream.
 *
 * @param any {Object} Any object.
 */
function isStream(any) {
  // This is a hacky way of checking that the transport is a stream-like
  // object. We unfortunately can't use `instanceof Stream` checks since
  // some libraries (e.g. websocket-stream) return streams which don't
  // inherit from it.
  return !!(any && any.pipe);
}

/**
 * Get a message, asserting that it exists.
 *
 * @param svc {Service} The protocol to look into.
 * @param name {String} The message's name.
 */
function getExistingMessage(svc, name) {
  var msg = svc.getMessage(name);
  if (!msg) {
    throw new Error(f('unknown message: %s', name));
  }
  return msg;
}

/**
 * Helper to expand an argument if a function.
 *
 */
function expandOption(opt, args) {
  if (opt === undefined) {
    return args[0];
  }
  if (typeof opt == 'function') {
    return opt.apply(undefined, args);
  }
  return opt;
}

/**
 * Middleware logic.
 *
 * This is used both in clients and servers to intercept call handling (e.g. to
 * populate headers, do access control).
 *
 * @param fns {Array} Array of middleware functions.
 * @param ctx {Object} Context used to call the middleware functions,
 * onTransition,
 * and onCompletion.
 * @param args {Array} Array of arguments. These will be used for all forward
 * middleware functions.
 * @param onTransition {Function} End of forward phase, the arguments this
 * function passes to its callback will be used for the backward phase. The
 * first of these arguments must be an (eventual) error. This function is
 * guaranteed to be called at most once.
 * @param onCompletion {Function} Final handler, it must accept the backward
 * phase arguments. This function is guaranteed to be only at most once.
 * @param onError {Function} Error handler, called if an intermediate callback
 * is called multiple times.
 */
function chainFunctions(fns, ctx, args, onTransition, onCompletion, onError) {
  var cbs = [];
  forward(0);

  function forward(pos) {
    var isDone = false;
    if (pos < fns.length) {
      fns[pos].apply(ctx, args.concat(function (err, cb) {
        if (isDone) {
          onError(new Error('duplicate middleware forward call'));
          return;
        }
        isDone = true;
        if (err) {
          // Stop the forward phase, bypass the handler, and start the backward
          // phase. Note that we ignore any callback argument in this case.
          args = [err];
          backward();
          return;
        }
        if (cb) {
          cbs.push(cb);
        }
        forward(++pos);
      }));
    } else {
      // Done with the middleware forward functions, call the handler.
      onTransition.apply(ctx, args.concat(function () {
        if (isDone) {
          onError(new Error('duplicate transition call'));
          return;
        }
        isDone = true;
        args = []; // Reset arguments.
        var numArgs = arguments.length;
        var i = 0;
        while (i < numArgs) {
          args.push(arguments[i++]);
        }
        process.nextTick(backward);
      }));
    }
  }

  function backward() {
    var cb = cbs.pop();
    if (cb) {
      var isDone = false;
      cb.apply(ctx, args.concat(function (err) {
        if (isDone) {
          onError(new Error('duplicate middleware backward call'));
          return;
        }
        if (err) {
          // Substitute the error. Note however that we don't allow removing
          // the error entirely. Once we enter the error flow, it isn't
          // possible to send a non-error response back.
          args[0] = err;
        }
        isDone = true;
        backward();
      }));
    } else {
      // Done with all middleware calls.
      onCompletion.apply(ctx, args);
    }
  }
}


module.exports = {
  HANDSHAKE_REQUEST_TYPE: HANDSHAKE_REQUEST_TYPE,
  HANDSHAKE_RESPONSE_TYPE: HANDSHAKE_RESPONSE_TYPE,
  Message: Message,
  Registry: Registry,
  Service: Service,
  discoverProtocol: discoverProtocol,
  streams: {
    FrameDecoder: FrameDecoder,
    FrameEncoder: FrameEncoder,
    NettyDecoder: NettyDecoder,
    NettyEncoder: NettyEncoder
  }
};
