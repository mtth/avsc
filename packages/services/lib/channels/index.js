/* jshint esversion: 6, node: true */

/**
 * Channels:
 *
 * Channel implementors should also support discovery calls: when the input
 * packet is null, the callback should return the list of services accessible
 * via this channel.
 */

'use strict';

const {Context} = require('../context');
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
 *
 * Only services present when the router is instantiated can be routed to. If a
 * packet comes in for an unrecognized service, the router will respond with an
 * error code of `ERR_AVRO_SERVICE_NOT_FOUND`.
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
    this._channel = (ctx, preq, cb) => {
      if (ctx.cancelled) {
        return;
      }
      if (!this._ready) {
        d('Router not yet ready, queuing call %s.', preq ? preq.id : '*');
        const cleanup = ctx.onCancel(() => {
          this.removeListener('ready', onReady);
        });
        const onReady = () => {
          if (cleanup()) {
            this._channel(ctx, preq, cb);
          }
        };
        this.once('ready', onReady);
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
      chan(ctx, preq, cb);
    };
    // Delay processing such that event listeners can be added first.
    process.nextTick(() => {
      let pending = chans.length;
      chans.forEach((chan) => {
        chan(new Context(), null, (err, svcs) => {
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
        this._setupChannel();
      })
      .on('fail', () => {
        d('Exhausted refresh attempts, giving up.');
        this._refreshError = new Error('exhausted refresh attempts');
      });

    this._setupChannel();
  }

  _setupChannel() {
    this._channelProvider((err, chan, ...args) => {
      if (err) {
        d('Error while refreshing channel, giving up: %s', err);
        this._refreshError = err;
        return;
      }
      chan(new Context(), null, (err, svcs) => {
        if (err) {
          d('Error on fresh channel, retrying shortly: %s', err);
          this._refreshBackoff.backoff();
          return;
        }
        this._activeArgs = args;
        this._activeChannel = chan; // TODO: Wrap in heartbeat.
        this._attempts = 1;
        this._refreshBackoff.reset();
        d('Monitor ready.');
        this.emit('up', ...args);
      });
    });
  }

  _teardownChannel() {
    if (!this._activeArgs) {
      return;
    }
    const args = this._activeArgs;
    this._activeArgs = null;
    this._activeChannel = null;
    this.emit('down', ...args);
  }

  get channel() {
    return (ctx, preq, cb) => {
      if (ctx.cancelled) {
        return;
      }
      if (!this._activeChannel) {
        const err = this._refreshError;
        if (err) {
          cb(new SystemError('ERR_AVRO_NO_AVAILABLE_CHANNEL', err));
        } else {
          d('Monitor not yet ready, queuing call %s.', preq ? preq.id : '*');
          const cleanup = ctx.onCancel(() => {
            this.removeListener('up', onUp);
          });
          const onUp = () => {
            if (cleanup()) {
              this.channel(ctx, preq, cb);
            }
          };
          this.once('up', onUp);
        }
        return;
      }
      this._activeChannel(ctx, preq, (err, pres) => {
        const args = this._activeArgs;
        if (err && args && this._isFatal(err) && !this._refreshError) {
          this._teardownChannel();
          this._refreshBackoff.backoff();
        }
        cb(err, pres);
      });
    };
  }

  destroy() {
    this._refreshError = new Error('destroyed');
    this._teardownChannel();
  }
}

module.exports = {
  Monitor,
  NettyClientBridge,
  NettyServerBridge,
  Router,
};
