/* jshint esversion: 6, node: true */

'use strict';

const {Channel, Packet} = require('./channel');
const {Decoder} = require('./service');
const {Trace} = require('./trace');
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
  constructor(req, hdrs) {
    this.request = req;
    this.headers = hdrs || {};
  }
}

class WrappedResponse {
  constructor(res, err, hdrs) {
    this.response = res;
    this.error = err;
    this.headers = hdrs || {};
  }

  get systemError() {
    return this.error && this.error.string;
  }
}

class Client {
  constructor(svc) {
    this.service = svc;
    this._channel = null;
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

  channel(chan) {
    if (chan === undefined) {
      return this._channel;
    }
    if (!Channel.isChannel(chan)) {
      throw new Error(`not a channel: ${chan}`);
    }
    this._channel = chan;
    return this;
  }

  call(trace, msgName, req, mws, cb) {
    const svc = this.service;
    const msg = svc.messages.get(msgName);
    if (!msg) {
      throw new Error(`no such message: ${msgName}`);
    }
    if (typeof cb != 'function') {
      throw new Error(`not a function: ${cb}`);
    }
    const cc = new CallContext(trace, msg);
    cc.client = this;
    if (trace.expired) {
      d('Not emitting message, trace has already expired.');
      process.nextTick(whenExpired);
      return;
    }
    const cleanup = trace.whenExpired(whenExpired);
    const wreq = new WrappedRequest(req);
    const wres = new WrappedResponse();
    chain(
      cc,
      wreq,
      wres,
      mws.concat(this._middlewares),
      (prev) => {
        const id = utils.randomId();
        const preq = new Packet(id, svc, null, wreq.headers);
        try {
          preq.body = Buffer.concat([
            utils.stringType.toBuffer(msg.name),
            msg.request.toBuffer(wreq.request),
          ]);
        } catch (err) {
          d('Unable to encode request: %s', err);
          prev(new SystemError('ERR_BAD_REQUEST', err));
          return;
        }
        d('Sending request packet %s...', id);
        if (!this._channel) {
          d('No channel to send request packet %s.', id);
          prev(new SystemError('ERR_NO_AVAILABLE_CHANNEL'));
          return;
        }
        this._channel.call(cc.trace, preq, (err, pres) => {
          if (err) {
            prev(SystemError.orCode('ERR_CHANNEL_FAILURE', err));
            return;
          }
          const serverSvc = pres.service;
          let decoder = this._decoders.get(serverSvc.hash);
          if (!decoder) {
            try {
              decoder = new Decoder(svc, serverSvc);
            } catch (err) {
              prev(new SystemError('ERR_INCOMPATIBLE_PROTOCOL', err));
              return;
            }
            d(
              'Adding client decoder for server service %s (%s).',
              serverSvc.name, toHex(serverSvc.hash)
            );
            this._decoders.set(serverSvc.hash, decoder);
          }
          d('Received response packet %s!', id);
          Object.assign(wres.headers, pres.headers);
          const buf = pres.body;
          try {
            if (buf[0]) { // Error.
              wres.error = decoder.decodeError(msg.name, buf.slice(1));
            } else {
              wres.response = decoder.decodeResponse(msg.name, buf.slice(1));
            }
          } catch (err) {
            d('Unable to decode response packet %s: %s', id, err);
            prev(new SystemError('ERR_CORRUPT_RESPONSE', err));
            return;
          }
          prev();
        });
      },
      onResponse
    );

    function whenExpired(err) {
      cb.call(cc, {string: SystemError.orCode('ERR_TRACE_EXPIRED', err)});
    }

    function onResponse(err) {
      if (!cleanup()) {
        return;
      }
      if (err) {
        wres.error = {string: SystemError.orCode('ERR_APPLICATION', err)};
      }
      cb.call(cc, wres.error, wres.response);
    }
  }

  emitMessage(trace, ...mws) {
    if (!Trace.isTrace(trace)) {
      throw new Error(`missing or invalid trace: ${trace}`);
    }
    return Object.assign(
      Object.create(this._emitterProto),
      {_trace$: trace, _middlewares$: mws }
    );
  }

  get _isClient() {
    return true;
  }

  static isClient(any) {
    return !!(any && any._isClient);
  }
}

function messageEmitter(msg) {
  return function(...reqArgs) {
    const req = {};
    const fields = msg.request.fields;
    for (const field of fields) {
      if (typeof reqArgs[0] == 'function') {
        break;
      }
      req[field.name] = reqArgs.shift();
    }
    const trace = this._trace$;
    const client = this._client$;
    const mws = this._middlewares$;
    const cb = reqArgs[0];
    if (!cb) {
      return client.call(trace, msg.name, req, mws);
    }
    client.call(trace, msg.name, req, mws, function (err, res) {
      const resArgs = [];
      for (const type of msg.error.types) {
        resArgs.push(err ? err[type.branchName] : null);
      }
      resArgs.push(res);
      cb.apply(this, resArgs);
    });
  };
}

class Server {
  constructor(svc, chanConsumer) {
    this.service = svc;
    this._middlewares = [];
    this._receivers = new Map();
    this._decoders = new Map();
    this._listenerProto = {_server$: this};
    for (const msg of svc.messages.values()) {
      this._listenerProto[msg.name] = messageListener(msg);
    }
    this._handler = (trace, preq, cb) => {
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
          cb(new SystemError('ERR_INCOMPATIBLE_PROTOCOL' , err));
          return;
        }
        d(
          'Adding server decoder for client service %s (%s).',
          clientSvc.name, toHex(clientSvc.hash)
        );
        this._decoders.set(clientSvc.hash, decoder);
      }
      const wreq = new WrappedRequest(null, preq.headers);
      const wres = new WrappedResponse();
      try {
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
        done(new SystemError('ERR_CORRUPT_REQUEST', cause));
        return;
      }
      const msg = cc.message;
      if (!msg) {
        d('Handling ping message.');
        cb(null, new Packet(id, svc, Buffer.alloc(1)));
        return;
      }
      const receiver = this._receivers.get(msg.name);
      let mws = this._middlewares;
      if (receiver && receiver.middlewares.length) {
        mws = mws.concat(receiver.middlewares);
      }
      chain(cc, wreq, wres, mws, (prev) => {
        if (!receiver) {
          const cause = new Error(`no handler for ${msg.name}`);
          prev(new SystemError('ERR_NOT_IMPLEMENTED', cause));
          return;
        }
        receiver.callback.call(cc, wreq.request, (err, rpcErr, rpcRes) => {
          if (err) {
            prev(err);
            return;
          }
          wres.error = rpcErr;
          wres.response = rpcRes;
          prev();
        });
      }, done);

      function done(err) {
        const msg = cc.message;
        let bufs;
        if (!err && msg) {
          try {
            if (wres.error !== undefined) {
              bufs = [Buffer.from([1]), msg.error.toBuffer(wres.error)];
            } else {
              let res = wres.response;
              if (res === undefined && msg.oneWay) {
                res = null;
              }
              bufs = [Buffer.from([0]), msg.response.toBuffer(res)];
            }
          } catch (cause) {
            err = new SystemError('ERR_BAD_RESPONSE', cause);
          }
        }
        if (err) {
          err = SystemError.orCode('ERR_APPLICATION', err);
          bufs = [Buffer.from([1, 0]), utils.systemErrorType.toBuffer(err)];
        }
        d('Sending response packet %s!', id);
        cb(null, new Packet(id, svc, Buffer.concat(bufs), wres.headers));
      }
    };
  }

  use(fn) {
    this._middlewares.push(fn);
    return this;
  }

  channel() {
    return new Channel(this._handler);
  }

  client() {
    const client = new Client(this.service);
    client.channel(this.channel());
    return client;
  }

  onCall(msgName, mws, fn) {
    const msg = this.service.messages.get(msgName);
    if (!msg) {
      throw new Error(`no such message: ${msgName}`);
    }
    if (fn.length && fn.length < 2) {
      throw new Error('too few handler arguments');
    }
    this._receivers.set(msgName, {middlewares: mws, callback: fn});
    return this;
  }

  onMessage(...mws) {
    return Object.assign(
      Object.create(this._listenerProto),
      {_middlewares$: mws}
    );
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
    let callback;
    if (fn.length <= msg.request.fields.length) {
      // Useful for promise servers.
      callback = function (req) { return fn.apply(this, requestArgs(req)); };
    } else {
      callback = function (req, cb) {
        const reqArgs = requestArgs(req);
        reqArgs.push((...resArgs) => {
          for (const [i, type] of msg.error.types.entries()) {
            const arg = resArgs[i];
            if (arg) {
              if (i === 0 && typeof arg != 'string') { // System error.
                cb.call(this, arg);
              } else {
                cb.call(this, null, {[type.branchName]: arg});
              }
              return;
            }
          }
          cb.call(this, null, undefined, resArgs[msg.error.types.length]);
        });
        return fn.apply(this, reqArgs);
      };
    }
    return this._server$.onCall(msg.name, this._middlewares$, callback);
  };

  function requestArgs(req) {
    const args = [];
    for (const field of msg.request.fields) {
      args.push(req[field.name]);
    }
    return args;
  }
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
      forward(i + 1, fn ? cbs.concat(fn): cbs);
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

function toHex(str) {
  return Buffer.from(str, 'binary').toString('hex');
}

module.exports = {
  Client,
  Server,
};
