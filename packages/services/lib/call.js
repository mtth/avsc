/* jshint esversion: 6, node: true */

'use strict';

const {Channel, Packet, Trace, randomId} = require('./channel');
const {Decoder} = require('./service');
const utils = require('./utils');

const debug = require('debug');
const {DateTime} = require('luxon');

const {SystemError} = utils;
const d = debug('@avro/services:call');

/** The context used for all middleware and handler calls. */
class CallContext {
  constructor(trace, msg) {
    this.trace = trace;
    this.message = msg;
    this.client = null; // Populated on clients.
    this.server = null; // Populated on servers.
  }
}

class WrappedRequest {
  constructor(req, tags) {
    this.request = req;
    this.tags = tags || {};
  }
}

class WrappedResponse {
  constructor(res, err, tags) {
    this.response = res;
    this.error = err;
    this.tags = tags || {};
  }

  get systemError() {
    return this.error && this.error.string;
  }

  _setSystemError(code, cause) {
    this.error = {string: SystemError.orCode(code, cause)};
  }
}

class Client {
  constructor(svc) {
    this.channel = null;
    this.service = svc;
    this.tagTypes = {};
    this._decoders = new Map();
    this._emitterProto = {_client$: this};
    for (const msg of svc.messages.values()) {
      this._emitterProto[msg.name] = messageEmitter(msg);
    }
    this._middlewares = [];
  }

  use(fn) {
    this._middlewares.push(fn);
    return this;
  }

  emitMessage(trace, ...mws) {
    if (!Trace.isTrace(trace)) {
      throw new Error(`missing or invalid trace: ${trace}`);
    }
    const obj = Object.create(this._emitterProto);
    obj._trace$ = trace;
    obj._middlewares$ = mws;
    return obj;
  }

  _emitMessage(trace, mws, name, req, cb) {
    if (!trace.active) {
      d('Not emitting message, trace is already inactive.');
      process.nextTick(() => { cb(trace.deactivatedBy); });
      return;
    }
    const cleanup = trace.onceInactive(done);
    const svc = this.service;
    const msg = svc.messages.get(name); // Guaranteed defined.
    const cc = new CallContext(trace, msg);
    cc.client = this;
    const wreq = new WrappedRequest(req);
    const wres = new WrappedResponse();
    chain(
      cc,
      wreq,
      wres,
      mws.concat(this._middlewares),
      (prev) => {
        const id = randomId();
        const preq = new Packet(id, svc);
        try {
          preq.headers = serializeTags(wreq.tags, this.tagTypes);
          preq.body = Buffer.concat([
            utils.stringType.toBuffer(name),
            msg.request.toBuffer(wreq.request),
          ]);
        } catch (err) {
          d('Unable to encode request: %s', err);
          wres._setSystemError('ERR_AVRO_BAD_REQUEST', err);
          prev();
          return;
        }
        d('Sending request packet %s...', id);
        if (!this.channel) {
          d('No channel to send request packet %s.', id);
          wres._setSystemError('ERR_AVRO_NO_AVAILABLE_CHANNEL');
          prev();
          return;
        }
        this.channel.call(cc.trace, preq, (err, pres) => {
          if (err) {
            wres._setSystemError('ERR_AVRO_CHANNEL_FAILURE', err);
            prev();
            return;
          }
          const serverSvc = pres.service;
          let decoder = this._decoders.get(serverSvc.hash);
          if (!decoder) {
            try {
              decoder = new Decoder(svc, serverSvc);
            } catch (err) {
              wres._setSystemError('ERR_AVRO_INCOMPATIBLE_PROTOCOL', err);
              prev();
              return;
            }
            d('Adding decoder for server service %j.', serverSvc.hash);
            this._decoders.set(serverSvc.hash, decoder);
          }
          d('Received response packet %s!', id);
          const buf = pres.body;
          try {
            const tags = deserializeTags(pres.headers, this.tagTypes);
            for (const key of Object.keys(tags)) {
              wres.tags[key] = tags[key];
            }
            if (buf[0]) { // Error.
              wres.error = decoder.decodeError(name, buf.slice(1));
            } else {
              wres.response = decoder.decodeResponse(name, buf.slice(1));
            }
          } catch (err) {
            d('Unable to decode response packet %s: %s', id, err);
            wres._setSystemError('ERR_AVRO_CORRUPT_RESPONSE', err);
          }
          prev();
        });
      },
      done
    );

    function done(err) {
      if (!cleanup()) {
        return;
      }
      if (err) {
        wres._setSystemError('ERR_AVRO_INTERNAL', err);
      }
      cb.call(cc, wres.error, wres.response);
    }
  }

  get _isClient() {
    return true;
  }

  static isClient(any) {
    return !!(any && any._isClient);
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
      this._trace$,
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
    this.tagTypes = {};
    this.service = svc;
    this._middlewares = [];
    this._handlers = new Map();
    this._decoders = new Map();
    this._listenerProto = {_server$: this};
    for (const msg of svc.messages.values()) {
      this._listenerProto[msg.name] = messageListener(msg);
    }
    this.channel = new Channel((trace, preq, cb) => {
      const id = preq.id;
      const cc = new CallContext(trace);
      cc.server = this;
      d('Received request packet %s.', id);

      const clientSvc = preq.service;
      let decoder = this._decoders.get(clientSvc.hash);
      if (!decoder && clientSvc.messages.size) {
        try {
          decoder = new Decoder(clientSvc, svc);
        } catch (err) {
          cb(new SystemError('ERR_AVRO_INCOMPATIBLE_PROTOCOL' , err));
          return;
        }
        d('Adding decoder for client service %j.', clientSvc.hash);
        this._decoders.set(clientSvc.hash, decoder);
      }
      const tagTypes = this.tagTypes;

      const wreq = new WrappedRequest();
      const wres = new WrappedResponse();
      try {
        const tags = deserializeTags(preq.headers, tagTypes);
        for (const key of Object.keys(tags)) {
          wreq.tags[key] = tags[key];
        }
        const obj = utils.stringType.decode(preq.body);
        if (obj.offset < 0) {
          throw new Error('unable to decode message name');
        }
        if (obj.value) { // Not a ping message.
          const msg = svc.messages.get(obj.value);
          if (!msg) {
            throw new Error(`no such message: ${msg.name}`);
          }
          const body = preq.body.slice(obj.offset);
          cc.message = msg;
          wreq.request = decoder.decodeRequest(msg.name, body);
        }
      } catch (cause) {
        d('Unable to decode request packet %s: %s', id, cause);
        wres._setSystemError('ERR_AVRO_CORRUPT_REQUEST', cause);
        done();
        return;
      }
      const msg = cc.message;
      if (!msg) {
        d('Handling ping message.');
        cb(null, new Packet(id, svc, Buffer.alloc(1)));
        return;
      }
      const handler = this._handlers.get(msg.name);
      if (handler) {
        d('Dispatching packet %s to handler for %j...', id, msg.name);
        handler.call(cc, wreq, wres, done);
      } else {
        d('Routing packet %s to placeholder handler for %j...', id, msg.name);
        chain(cc, wreq, wres, this._middlewares, (prev) => {
          const cause = new Error(`no handler for ${msg.name}`);
          wres._setSystemError('ERR_AVRO_NOT_IMPLEMENTED', cause);
          prev();
        }, done);
      }

      function done(err) {
        if (err) {
          // This will happen if an error was returned by a middleware.
          err = SystemError.orCode('ERR_AVRO_INTERNAL', err);
        }
        let bufs, headers;
        try {
          headers = serializeTags(wres.tags, tagTypes);
          const msg = cc.message;
          if (msg) {
            if (wres.error !== undefined) {
              bufs = [Buffer.from([1]), msg.error.toBuffer(wres.error)];
            } else {
              bufs = [Buffer.from([0]), msg.response.toBuffer(wres.response)];
            }
          }
        } catch (cause) {
          err = new SystemError('ERR_AVRO_BAD_RESPONSE', cause);
        }
        if (err) {
          bufs = [Buffer.from([1, 0]), utils.systemErrorType.toBuffer(err)];
        }
        d('Sending response packet %s!', id);
        cb(null, new Packet(id, svc, Buffer.concat(bufs), headers));
      }
    });
  }

  use(fn) {
    this._middlewares.push(fn);
    return this;
  }

  _onMessage(mws, name, fn) {
    mws = this._middlewares.concat(mws);
    this._handlers.set(name, function (wreq, wres, cb) {
      chain(this, wreq, wres, mws, (prev) => {
        d('Starting %j handler call...', name);
        fn.call(this, wreq.request, (err, res) => {
          d('Done calling %j handler.', name);
          wres.error = err;
          wres.response = res;
          prev();
        });
      }, cb);
    });
  }

  onMessage(...mws) {
    const obj = Object.create(this._listenerProto);
    obj._middlewares$ = mws;
    return obj;
  }

  get _isServer() {
    return true;
  }

  static isServer(any) {
    return !!(any && any._isServer);
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

function chain(cc, wreq, wres, mws, turn, end) {
  process.nextTick(() => { forward(0, []); });

  function forward(i, cbs) {
    if (wres.response !== undefined || wres.error !== undefined) {
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
    mw.call(cc, wreq, wres, (err, fn) => {
      if (err) {
        backward(err, cbs);
        return;
      }
      forward(i + 1, fn && !cc.message.oneWay ? cbs.concat(fn): cbs);
    });
  }

  function backward(err, cbs) {
    const cb = cbs && cbs.pop();
    if (!cb) {
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
  for (const key of Object.keys(tagTypes)) {
    const buf = headers[key];
    if (buf !== undefined) {
      tags[key] = tagTypes[key].fromBuffer(buf);
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
  Client,
  Server,
};
