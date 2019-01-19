/* jshint esversion: 6, node: true */

'use strict';

const {NettyClientBridge, NettyServerBridge} = require('./netty');
const {SystemError} = require('../types');

const backoff = require('backoff');
const debug = require('debug');
const {EventEmitter} = require('events');
const {DateTime, Duration} = require('luxon');

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
        d('Router not yet ready, queuing call %s.', preq.id);
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

/**
 * Resilient channel.
 *
 * Note that we don't retry calls since some might have side-effects...
 */
// TODO: Add heartbeat option.
// TODO: Add option to retry calls and spread them over time.
class Watcher extends EventEmitter {
  constructor(fn, {isFatal, refreshBackoff} = {}) {
    super();
    this._refresh = fn;
    this._activeArgs = null;
    this._isFatal = isFatal || ((err) => err.code === 'ERR_AVRO_CHANNEL_FAILURE');
    this._attempts = 0;
    this._nextAttempt = null;
    this._refreshBackoff = (refreshBackoff || backoff.fibonacci())
      .on('backoff', (num, delay) => {
        this._attempts = num + 1;
        d('Backing off refresh #%s by %sms.', this._attempts, delay);
        this._nextAttempt = DateTime.local().plus(Duration.fromMillis(delay));
        this._activeArgs = null;
      })
      .on('ready', (num) => {
        d('Starting refresh attempt...');
        this._refreshChannel((err) => {
          if (err) {
            if (this._isFatal(err)) {
              d('Error while refreshing channel, retrying shortly: %s', err);
              this._refreshBackoff.backoff();
            } else {
              d('Error while refreshing channel, giving up: %s', err);
            }
            return;
          }
          this._attempts = 1;
          this._refreshBackoff.reset();
        });
      })
      .on('fail', () => { d('Exhausted refresh attempts, giving up.'); });
    this._refreshChannel((err) => {
      if (err) {
        d('Startup failed, refreshing: %s', err);
        this._refreshBackoff.backoff();
      }
    });
  }

  _refreshChannel(cb) {
    this._refresh((err, ...args) => {
      if (err) {
        d('Error while refreshing, giving up: %s', err);
        this.emit('error', err);
        return;
      }
      this._attempts = 1;
      this._activeArgs = args;
      d('Watcher is now ready.');
      this.emit('ready');
      this._activeArgs[0](null, cb); // Make sure the channel is available.
    });
  }

  get channel() {
    return (preq, cb) => {
      if (!this._activeArgs) {
        if (!this._attempts) { // Watcher is starting up for the first time.
          d('Watcher not yet ready, queuing call %s.', preq.id);
          this.once('ready', () => { this.channel(preq, cb); });
          return;
        }
        d('Watcher is currently refreshing, declining packet %s.', preq.id);
        const err = new Error(`no channel after ${this._attempts} attempt(s)`);
        err.retryAfter = this._nextAttempt;
        cb(new SystemError('ERR_AVRO_NO_AVAILABLE_CHANNEL', err));
        return;
      }
      this._activeArgs[0](preq, (err, pres) => {
        if (err && this._activeArgs && this._isFatal(err)) {
          this.emit('down', ...this._activeArgs);
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
  Watcher,
};
