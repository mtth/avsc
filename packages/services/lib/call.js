/* jshint esversion: 6, node: true */

'use strict';

const {Context, idGenerator} = require('./context');
const types = require('./types');

const debug = require('debug');
const {RequestPacket, ResponsePacket, SystemError} = types;

const d = debug('avro:services:client');

class Call {
  constructor(ctx, msg) {
    this._context = ctx;
    this._message = msg;
    this.request = undefined;
    this.response = undefined;
    this.error = undefined;
    this.tags = {};
  }

  get context() {
    return this._context;
  }

  get message() {
    return this._message;
  }

  get systemError() {
    return this.error && this.error.string;
  }

  _setSystemError(code, cause) {
    this.error = {string: new SystemError(code, cause)};
  }
}

class Decoder {
  constructor(clientSvc, serverSvc) {
    const rs = new Map();
    for (const clientMsg of clientSvc.messages.values()) {
      const name = clientMsg.name;
      const serverMsg = serverSvc.messages.get(name);
      if (!serverMsg) {
        throw new Error(`missing server message: ${name}`);
      }
      if (serverMsg.oneWay !== clientMsg.oneWay) {
        throw new Error(`inconsistent one-way message: ${name}`);
      }
      addResolver(name + '?', serverMsg.request, clientMsg.request);
      addResolver(name + '*', clientMsg.error, serverMsg.error);
      addResolver(name + '!', clientMsg.response, serverMsg.response);
    }
    this._resolvers = rs;
    this._clientService = clientSvc;
    this._serverService = serverSvc;

    function addResolver(key, rtype, wtype) {
      if (!rtype.equals(wtype)) {
        rs.set(key, rtype.createResolver(wtype));
      }
    }
  }

  decodeRequest(name, buf) {
    const msg = this._serverService.messages.get(name);
    return msg.request.fromBuffer(buf, this._resolvers.get(name + '?'));
  }

  decodeError(name, buf) {
    const msg = this._clientService.messages.get(name);
    return msg.error.fromBuffer(buf, this._resolvers.get(name + '*'));
  }

  decodeResponse(name, buf) {
    const msg = this._clientService.messages.get(name);
    return msg.response.fromBuffer(buf, this._resolvers.get(name + '!'));
  }
}

class Client {
  constructor(svc) {
    this.channel = null;
    this._tagTypes = new Map();
    this._service = svc;
    this._decoders = new Map();
    this._idGenerator = idGenerator();
    this._emitterProto = {_client$: this};
    for (const msg of svc.messages.values()) {
      this._emitterProto[msg.name] = messageEmitter(msg);
    }
    this._middlewares = [];
  }

  get service() {
    return this._service;
  }

  get tagTypes() {
    return this._tagTypes;
  }

  use(fn) {
    this._middlewares.push(fn);
    return this;
  }

  ping(ctx, cb) {
    cb = cb || throwIfError;
    const id = this._idGenerator.next().value;
    if (!this.channel) {
      d('No channel available to send ping packet %s.', id);
      cb(new SystemError('ERR_AVRO_NO_AVAILABLE_CHANNEL'));
      return;
    }
    const reqPkt = new RequestPacket(id, '', Buffer.alloc(0));
    this.channel.call(ctx, this._service, reqPkt, (err) => {
      if (err) {
        cb(new SystemError('ERR_AVRO_CHANNEL_FAILURE', err));
        return;
      }
      cb();
    });
  }

  emitMessage(ctx, ...mws) {
    if (!ctx) {
      throw new Error('missing context');
    }
    const obj = Object.create(this._emitterProto);
    obj._context$ = ctx;
    obj._middlewares$ = mws;
    return obj;
  }

  _emitMessage(ctx, mws, name, req, cb) {
    const svc = this._service;
    const msg = svc.messages.get(name); // Guaranteed defined.
    const call = new Call(ctx, msg);
    call.request = req;
    chain(
      this,
      call,
      mws.concat(this._middlewares),
      (prev) => {
        const id = this._idGenerator.next().value;
        const reqPkt = new RequestPacket(id, name);
        try {
          reqPkt.headers = serializeTags(call.tags, this._tagTypes);
          reqPkt.body = msg.request.toBuffer(call.request);
        } catch (err) {
          d('Unable to encode request: %d', err);
          call._setSystemError('ERR_AVRO_BAD_REQUEST', err);
          prev();
          return;
        }
        d('Sending request packet %s...', id);
        if (!this.channel) {
          d('No channel to send request packet %s.', id);
          call._setSystemError('ERR_AVRO_NO_AVAILABLE_CHANNEL');
          prev();
          return;
        }
        this.channel.call(ctx, svc, reqPkt, (err, serverSvc, resPkt) => {
          if (err) {
            call._setSystemError('ERR_AVRO_CHANNEL_FAILURE', err);
            prev();
            return;
          }
          let decoder = this._decoders.get(serverSvc.hash);
          if (!decoder) {
            try {
              decoder = new Decoder(svc, serverSvc);
            } catch (err) {
              call._setSystemError('ERR_AVRO_INCOMPATIBLE_PROTOCOL', err);
              prev();
              return;
            }
            d('Adding decoder for server service %j.', serverSvc.hash);
            this._decoders.set(serverSvc.hash, decoder);
          }
          if (ctx.cancelled) {
            d('Call %s was cancelled, ignoring incoming request packet.', id);
            return;
          }
          d('Received response packet %s!', id);
          const buf = resPkt.body;
          try {
            const tags = deserializeTags(resPkt.headers, this._tagTypes);
            for (const key of Object.keys(tags)) {
              call.tags[key] = tags[key];
            }
            if (buf[0]) { // Error.
              call.error = decoder.decodeError(name, buf.slice(1));
            } else {
              call.response = decoder.decodeResponse(name, buf.slice(1));
            }
          } catch (err) {
            d('Unable to decode response packet %s: %s', id, err);
            call._setSystemError('ERR_AVRO_CORRUPT_RESPONSE', err);
          }
          prev();
        });
      },
      done
    );

    function done(err) {
      if (err) {
        call._setSystemError('ERR_AVRO_INTERNAL', err);
      }
      cb.call(call.context, call.error, call.response);
    }
  }
}

function messageEmitter(msg) {
  return function(...args) {
    const req = {};
    const fields = msg.request.fields;
    for (const [i, field] of fields.entries()) {
      req[field.name] = args[i];
    }
    return this._client$._emitMessage(
      this._context$,
      this._middlewares$,
      msg.name,
      req,
      flatteningErr(args[fields.length] || throwIfError)
    );
  };

  function flatteningErr(cb) {
    return function (err, res) {
      const args = [];
      for (const type of msg.error.types) {
        args.push(err ? err[type.branchName] : undefined);
      }
      args.push(res);
      return cb.apply(this, args);
    };
  }
}

class Server {
  constructor(svc, chanConsumer) {
    this._tagTypes = {};
    this._service = svc;
    this._middlewares = [];
    this._handlers = new Map();
    this._decoders = new Map();
    this._listenerProto = {_server$: this};
    for (const msg of svc.messages.values()) {
      this._listenerProto[msg.name] = messageListener(msg);
    }
    this._channel = (clientSvc, reqPkt, cb) => {
      const id = reqPkt.id;
      if (!reqPkt.messageName) { // Ping message.
        cb(null, svc, new ResponsePacket(id, Buffer.alloc(1)));
        return;
      }

      let decoder = this._decoders.get(clientSvc.hash);
      if (!decoder) {
        try {
          decoder = new Decoder(clientSvc, svc);
        } catch (err) {
          cb(err);
          return;
        }
        d('Adding decoder for client service %j.', clientSvc.hash);
        this._decoders.set(clientSvc.hash, decoder);
      }
      const tagTypes = this._tagTypes;

      d('Received request packet %s!', id);
      const msg = svc.messages.get(reqPkt.messageName);
      const call = new Call(new Context(), msg);
      try {
        if (!msg) {
          throw new Error(`no such message: ${reqPkt.messageName}`);
        }
        const tags = deserializeTags(reqPkt.headers, tagTypes);
        for (const key of Object.keys(tags)) {
          call.tags[key] = tags[key];
        }
        call.request = decoder.decodeRequest(msg.name, reqPkt.body);
      } catch (cause) {
        d('Unable to decode request packet %s: %s', id, cause);
        call._setSystemError('ERR_AVRO_CORRUPT_REQUEST', cause);
        done();
        return;
      }

      const handler = this._handlers.get(msg.name);
      if (handler) {
        d('Dispatching packet %s to handler for %j...', id, msg.name);
        handler(call, done);
      } else {
        d('Routing packet %s to placeholder handler for %j...', id, msg.name);
        chain(this, call, this._middlewares, (prev) => {
          call._setSystemError('ERR_AVRO_NOT_IMPLEMENTED');
          prev();
        }, done);
      }

      function done(err) {
        if (err) {
          // This will happen if an error was returned by a middleware.
          call._setSystemError('ERR_AVRO_INTERNAL', err);
        }
        const msg = call.message;
        const byte = Buffer.alloc(1);
        let buf, headers;
        try {
          headers = serializeTags(call.tags, tagTypes);
          if (call.error !== undefined) {
            byte[0] = 1;
            buf = msg.error.toBuffer(call.error);
          } else {
            buf = msg.response.toBuffer(call.response);
          }
        } catch (cause) {
          call._setSystemError('ERR_AVRO_BAD_RESPONSE', cause);
          byte[0] = 1;
          buf = msg.error.toBuffer(call.error);
        }
        d('Sending response packet %s!', id);
        const body = Buffer.concat([byte, buf]);
        cb(null, svc, new ResponsePacket(id, body, headers));
      }
    };
  }

  get service() {
    return this._service;
  }

  get tagTypes() {
    return this._tagTypes;
  }

  get channel() {
    return this._channel;
  }

  use(fn) {
    this._middlewares.push(fn);
    return this;
  }

  _onMessage(mws, name, fn) {
    this._handlers.set(name, (call, cb) => {
      chain(
        this,
        call,
        this._middlewares.concat(mws),
        (prev) => {
          d('Starting %j handler call...', name);
          fn.call(call.context, call.request, (err, res) => {
            d('Done calling %j handler.', name);
            call.error = err;
            call.response = res;
            prev();
          });
        },
        cb
      );
    });
  }

  onMessage(...mws) {
    const obj = Object.create(this._listenerProto);
    obj._middlewares$ = mws;
    return obj;
  }
}

function messageListener(msg) {
  return function (fn) {
    this._server$._onMessage(this._middlewares$, msg.name, function (req, cb) {
      const reqArgs = [];
      for (const field of msg.request.fields) {
        reqArgs.push(req[field.name]);
      }
      reqArgs.push((...resArgs) => {
        for (const [i, type] of msg.error.types.entries()) {
          let arg = resArgs[i];
          if (arg) {
            if (i === 0) {
              arg = new SystemError('ERR_AVRO_APPLICATION', arg);
            }
            cb.call(this, {[type.branchName]: arg});
            return;
          }
        }
        cb.call(this, undefined, resArgs[msg.error.types.length]);
      });
      fn.apply(this, reqArgs);
    });
    return this._server$;
  };
}

function chain(ctx, call, mws, turn, end) {
  let cleanup;
  cleanup = call.context.onCancel((err) => { // err guaranteed SystemError.
    call.error = {string: err};
    cleanup = null;
    end(); // Skip everything, exit the chain.
  });
  forward(0, []);

  function forward(i, cbs) {
    if (call.context.cancelled) {
      return;
    }
    if (call.response !== undefined || call.error !== undefined) {
      // The call has been answered...
      backward(null, cbs);
      return;
    }
    const mw = mws[i];
    if (!mw) {
      // No more middleware.
      turn((err) => { backward(err, cbs); });
      return;
    }
    mw.call(ctx, call, (err, fn) => {
      if (err) {
        backward(err, cbs);
        return;
      }
      forward(i + 1, fn && !call.message.oneWay ? cbs.concat(fn): cbs);
    });
  }

  function backward(err, cbs) {
    if (!cleanup) {
      return; // Call was already cancelled.
    }
    const cb = cbs && cbs.pop();
    if (!cb) {
      cleanup();
      end(err);
      return;
    }
    cb(err, (err) => { backward(err, cbs); });
  }
}

function serializeTags(tags, tagTypes) {
  const headers = {};
  if (!tags) {
    return headers;
  }
  for (const key of Object.keys(tags)) {
    const type = tagTypes[key];
    if (!type) {
      // We throw an error here to avoid silently failing (unlike remote
      // headers, tags are under the same process' control).
      throw new Error(`unknown tag: ${key}`);
    }
    headers[key] = type.toBuffer(tags[key]);
  }
  return headers;
}

function deserializeTags(headers, tagTypes) {
  const tags = {};
  if (!headers) {
    return tags;
  }
  for (const key of Object.keys(headers)) {
    const type = tagTypes[key];
    if (type) {
      tags[key] = type.fromBuffer(headers[key]);
    } else {
      // Unlike the serialization case above, the client/server can't
      // necessarily control which headers its counterpart uses.
      d('Ignoring unknown tag %s', key);
    }
  }
  return tags;
}

function throwIfError(err) {
  if (err) {
    throw err;
  }
}

module.exports = {
  Call,
  Client,
  Server,
};
