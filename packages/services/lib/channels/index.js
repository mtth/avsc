/* jshint esversion: 6, node: true */

'use strict';

const {NettyClientBridge, NettyServerBridge} = require('./netty');
const {SystemError} = require('../types');

// const backoff = require('backoff');
const debug = require('debug');
const {EventEmitter} = require('events');

const d = debug('avro:services:channels');

/**
 * Router between channels for multiple distinct services.
 *
 * Calls are routed using services' qualified name, so they must be distinct
 * across the routed channels.
 */
class Router extends EventEmitter {
  constructor(chans) {
    if (!chans || !chans.length) {
      throw new Error('empty router');
    }
    super();
    this._channels = new Map();
    this._services = [];
    this._ready = false;
    this._channel = (preq, cb) => {
      if (!this._ready) {
        d('Router not yet ready, queuing call.');
        this.once('ready', () => { this._channel(preq, cb); });
        return;
      }
      if (preq === null) {
        cb(null, this._services.slice());
        return;
      }
      const name = preq.service.name;
      const chan = this._channels.get(name);
      if (!chan) {
        const cause = new Error(`no such service: ${name}`);
        cause.serviceName = name;
        cb(new SystemError('ERR_AVRO_SERVICE_NOT_FOUND', cause));
        return;
      }
      chan(preq, cb);
    };
    // Delay processing such that event listeners can be added first.
    process.nextTick(() => {
      let pending = chans.length;
      chans.forEach((chan) => {
        chan(null, (err, svcs) => {
          if (err) {
            this.emit('error', err);
            return;
          }
          for (const svc of svcs) {
            if (this._channels.has(svc.name)) {
              this.emit('error', new Error(`duplicate service: ${svc.name}`));
              return;
            }
            this._channels.set(svc.name, chan);
            this._services.push(svc);
          }
          this._ready = true;
          if (--pending === 0) {
            d('Router ready to route %s service(s).', this._services.length);
            this.emit('ready');
          }
        });
      });
    });
  }

  get channel() {
    return this._channel;
  }

  get serverServices() {
    return this._services.slice();
  }
}


// const watcher = new channels.Watcher()
//   .watch(((cb) => {
//   cb(null, chan, sock);
// }).on('up', (sock) => {
//   })
//   .on('down', (sock) => {
//   });

/**
 * Resilient channel.
 *
 * Note that we don't retry calls since some might have side-effects...
 */
// TODO: Add heartbeat option.
// TODO: Add option to retry calls and spread them over time.
class Watcher extends EventEmitter {
  constructor(provider, {isFatal, refreshBackoff} = {}) {
    this._provider = provider;
    this._isFatal = isFatal || ((err) => err.code === 'ERR_AVRO_CHANNEL_FAILURE');
    this._attempts = 0;
    this._nextAttempt = null;
    this._refreshBackoff = (refreshBackoff || backoff.fibonacci())
      .on('backoff', (num, delay) => {
        d('Channel refresh backoff #%s (%sms)...', num, delay);
        this._attempts = num;
        this._nextAttempt = DateTime.local().plus(Duration.fromMillis(delay));
      })
      .on('ready', () => {
        d('Channel refresh attempt #%s...', num);
        const chan = this._channelProvider();
        poll(chan, (err) => {
          if (err) {
          }
        });

        const bkf = this._refreshBackoff;

      });
  }

  _restart(cb) {
    this._provider((err, chan, ...args) => {
      if (err) {
        cb(err);
        return;
      }
    });
  }

  get channel() {
    return (preq, cb) => {
      if (!this._activeChannel) {
        const err = new Error(`no channel after ${this._attempts} attempt(s)`);
        err.retryAfter = this._nextAttempt;
        cb(new SystemError('ERR_AVRO_NO_AVAILABLE_CHANNEL', err));
        return;
      }
      this._channel(preq, (err, pres) => {
        if (err && this._isFatal(err)) { // Channel can't be reused...
          this._channel = null;
          this._refreshBackoff.backoff();
          return;
        }
        cb(err, pres);
      });
    };
  }
}

module.exports = {
  NettyClientBridge,
  NettyServerBridge,
  Router,
};
