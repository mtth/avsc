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
class Monitor extends EventEmitter {
  constructor(fn, {isFatal, refreshBackoff} = {}) {
    super();
    this._channelProvider = fn;
    this._activeChannel = null;
    this._activeArgs = null;
    this._isFatal = isFatal || ((e) => e.code === 'ERR_AVRO_CHANNEL_FAILURE');
    this._refreshError = null;

    this._attempts = 1;
    this._refreshBackoff = (refreshBackoff || backoff.fibonacci())
      .on('backoff', (num, delay) => {
        d('Scheduling refresh in %sms.', delay);
      })
      .on('ready', () => {
        d('Starting refresh attempt #%s...', this._attempts++);
        this._refreshChannel();
      })
      .on('fail', () => {
        d('Exhausted refresh attempts, giving up.');
        this._refreshError = new Error('exhausted refresh attempts');
      });

    this._refreshChannel();
  }

  _refreshChannel() {
    this._channelProvider((err, chan, ...args) => {
      if (err) {
        d('Error while refreshing channel, giving up: %s', err);
        this._refreshError = err;
        return;
      }
      chan(null, (err, svcs) => {
        if (err) {
          d('Error on fresh channel, retrying shortly: %s', err);
          this._refreshBackoff.backoff();
          return;
        }
        this._activeChannel = chan; // TODO: Wrap in heartbeat.
        this._activeArgs = args;
        this._attempts = 1;
        this._refreshBackoff.reset();
        d('Monitor ready.');
        this.emit('up', ...args);
      });
    });
  }

  get channel() {
    return (preq, cb) => {
      if (!this._activeChannel) {
        const err = this._refreshError;
        if (err) {
          cb(new SystemError('ERR_AVRO_NO_AVAILABLE_CHANNEL', err));
        } else {
          d('Monitor not yet ready, queuing call %s.', preq.id);
          this.once('up', () => { this.channel(preq, cb); });
        }
        return;
      }
      this._activeChannel(preq, (err, pres) => {
        const args = this._activeArgs;
        if (err && args && this._isFatal(err) && !this._refreshError) {
          this._activeChannel = null;
          this._activeArgs = null;
          this.emit('down', ...args);
          this._refreshBackoff.backoff();
        }
        cb(err, pres);
      });
    };
  }
}

module.exports = {
  Monitor,
  NettyClientBridge,
  NettyServerBridge,
  Router,
};
