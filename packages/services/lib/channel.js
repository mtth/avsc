/* jshint esversion: 6, node: true */

'use strict';

const {SystemError, mapOfStringType, randomId} = require('./utils');

const backoff = require('backoff');
const debug = require('debug');
const {EventEmitter} = require('events');

const d = debug('@avro/services:channel');

/**
 * A serialized message, sent and received via channels.
 *
 * Each packet's ID should be unique.
 */
class Packet {
  constructor(id, svc, body, headers, baggages) {
    this.id = id;
    this.service = svc;
    this.body = body;

    // String values, propagated from/to request and response headers.
    this.headers = headers || {};
    if (!mapOfStringType.isValid(this.headers)) {
      const err = new Error('bad header values');
      err.data = headers;
      throw err;
    }

    // String values, propagated from/to the context only for requests.
    this.baggages = baggages || {};
    if (!mapOfStringType.isValid(this.baggages)) {
      const err = new Error('bad baggage values');
      err.data = baggages;
      throw err;
    }
  }

  static ping(svc, headers) {
    return new Packet(-randomId(), svc, Buffer.from([0, 0]), {}, headers);
  }
}

/** A communication mechanism. */
class Channel extends EventEmitter {
  constructor(handler) {
    super();
    this._handler = handler || null;
    this.closed = false;
  }

  get opened() {
    return !!this._handler && !this.closed;
  }

  open(handler) {
    if (this._handler) {
      throw new Error('channel was already opened');
    }
    if (this.closed) {
      throw new Error('channel was already closed');
    }
    this._handler = handler;
    this.emit('_flush');
    this.emit('open');
  }

  close() {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.emit('_flush');
    this.emit('close');
  }

  /** If the deadline is (or becomes) expired, call will _not_ respond. */
  call(deadline, preq, cb) {
    if (deadline.expired) {
      d('Dropping packet request %s (expired deadline).', preq.id);
      this.emit('drop', deadline, preq.id);
      return;
    }
    if (this.closed) {
      cb(new SystemError('ERR_CHANNEL_CLOSED'));
      return;
    }
    if (!this._handler) {
      d('Buffering packet request %s.', preq.id);
      const onFlush = () => {
        cleanup();
        this.removeListener('_flush', onFlush);
        if (!deadline.expired) {
          this.call(deadline, preq, cb);
        }
      };
      const cleanup = deadline.whenExpired(onFlush);
      this.on('_flush', onFlush);
      return;
    }
    this.emit('requestPacket', preq, deadline);
    this._handler.call(this, deadline, preq, (err, pres) => {
      if (deadline.expired) {
        d('Dropping packet response %s (expired deadline).', preq.id);
        this.emit('drop', deadline, preq.id);
        return;
      }
      let systemErr;
      if (err) {
        systemErr = err = SystemError.orCode('ERR_CHANNEL_FAILURE', err);
      } else if (pres) {
        systemErr = SystemError.forPacket(pres);
      }
      if (systemErr) {
        this.emit('systemError', systemErr, deadline);
      }
      this.emit('responsePacket', pres, deadline);
      cb(err, pres);
    });
  }

  /** Similar to call, if the deadline has expired, ping will _not_ respond. */
  ping(deadline, svc, headers, cb) {
    if (!cb && typeof headers == 'function') {
      cb = headers;
      headers = undefined;
    }
    this.call(deadline, Packet.ping(svc, headers), (err, pres) => {
      if (err) {
        cb(err);
        return;
      }
      cb(null, pres.service, pres.headers);
    });
  }

  get _isChannel() {
    return true;
  }

  static isChannel(any) {
    return !!(any && any._isChannel);
  }
}

class RoutingChannel extends Channel {
  constructor(chans) {
    super();

    this._channels = [];
    this._channelCache = new Map(); // By service name.
    this._handler = (deadline, preq, cb) => {
      const clientSvc = preq.service;
      const name = clientSvc.name;
      const cache = this._channelCache;
      const chan = cache.get(name);
      if (chan) { // If we have a cached match, we use it directly.
        routeUsing(chan);
        return;
      }
      // Otherwise we ping all downstream channels and use the first one
      // (w.r.t. array order) which is compatible. Note that we ping all
      // channels since a single channel might be compatible with multiple
      // protocols (e.g. if itself is a routing channel).
      multiPing(deadline, clientSvc, this._channels, (err, chan) => {
        if (err) {
          cb(err);
          return;
        }
        cache.set(name, chan);
        chan.on('close', onClose);
        this.on('close', () => { chan.removeListener('close', onClose); });
        routeUsing(chan);

        function onClose() { // Called when the downstream channel closes.
          d('Channel for %s is now closed, purging.', name);
          cache.delete(name);
        }
      });

      function routeUsing(chan) {
        chan.call(deadline, preq, (err, pres) => {
          if (err && err.code === 'ERR_INCOMPATIBLE_PROTOCOL') {
            d('Channel for %s is not compatible, purging.', name);
            cache.delete(name);
          }
          cb(err, pres);
        });
      }
    };

    if (chans) {
      for (const chan of chans) {
        this.addDownstream(chan);
      }
    }
  }

  /** Add a downstream channel. */
  addDownstream(chan) {
    if (!Channel.isChannel(chan)) {
      throw new Error(`not a channel: ${chan}`);
    }
    if (chan.closed) {
      throw new Error('channel is already closed');
    }
    const chans = this._channels;
    if (~chans.indexOf(chan)) {
      throw new Error('duplicate channel');
    }
    chans.push(chan.on('close', onClose));
    this.once('close', () => { chan.removeListener('close', onClose); });

    function onClose() {
      chans.splice(chans.indexOf(chan), 1);
    }
  }

  static forServers(servers) {
    const chan = new RoutingChannel();
    for (const server of servers) {
      chan.addDownstream(server.channel());
    }
    return chan;
  }
}

class SelfRefreshingChannel extends Channel {
  constructor(provider, opts) {
    super();
    opts = opts || {};
    const refreshBackoff = opts.refreshBackoff || backoff.fibonacci();

    this._channelProvider = provider;
    this._activeChannel = null; // Activated below.
    this._refreshAttempts = 0;
    this._refreshBackoff = refreshBackoff
      .on('backoff', (num, delay) => {
        d('Scheduling refresh in %sms.', delay);
        this._refreshAttempts++;
      })
      .on('ready', () => {
        d('Starting refresh attempt #%s...', this._refreshAttempts);
        this._refreshChannel();
      })
      .on('fail', () => {
        d('Exhausted refresh attempts, giving up.');
        this.emit('error', new Error('exhausted refresh attempts'));
      });

    this.once('close', () => {
      if (this._activeChannel) {
        this._activeChannel.close();
      }
    });
    this._refreshChannel();
  }

  _refreshChannel() {
    if (this._activeChannel) {
      throw new Error('channel already active');
    }
    d('Refreshing active channel...');
    this._channelProvider((err, chan, ...args) => {
      if (err) {
        d('Error while opening channel: %s', err);
        if (!this.closed) {
          process.nextTick(() => { this._refreshBackoff.backoff(); });
        }
        return;
      }
      if (this.closed) {
        chan.close();
        return;
      }
      this._refreshAttempts = 0;
      this._refreshBackoff.reset();
      this._activeChannel = chan
        .on('error', (err) => { this.emit('error', err); })
        .once('close', () => {
          d('Active channel was closed.');
          this._activeChannel = null;
          this._handler = null;
          this.emit('down', ...args);
          if (!this.closed) {
            this._refreshChannel();
          }
        });
      this._handler = (deadline, preq, cb) => { chan.call(deadline, preq, cb); };
      d('Set active channel.');
      this.emit('_flush');
      this.emit('up', ...args);
    });
  }
}

function multiPing(deadline, svc, chans, cb) {
  const errs = [];
  let pending = chans.length;
  if (!pending) {
    cb(notFound(svc));
    return;
  }
  let match = null;
  for (const [idx, chan] of chans.entries()) {
    chan.ping(deadline, svc, onPing(idx, chan));
  }

  function onPing(idx, chan) {
    return (err) => {
      pending--;
      if (err && err.code !== 'ERR_INCOMPATIBLE_PROTOCOL') {
        errs.push(err);
        return;
      }
      if (!err && (!match || match[0] > idx)) {
        match = [idx, chan];
      }
      if (pending) {
        return;
      }
      if (errs.length) {
        cb(errs[0]); // TODO: Expose other errors?
        return;
      }
      if (!match) {
        cb(notFound(svc));
        return;
      }
      cb(null, match[1]);
    };
  }
}

function notFound(svc) {
  const cause = new Error(`no match for protocol ${svc.name}`);
  return new SystemError('ERR_INCOMPATIBLE_PROTOCOL', cause);
}

module.exports = {
  Channel,
  Packet,
  RoutingChannel,
  SelfRefreshingChannel,
};
