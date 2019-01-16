/* jshint esversion: 6, node: true */

// TODO: Document (and implement) channel APIs letting ping messages through
// (without any impact on connections). This will let us implement a
// `discoverProtocol` method.

'use strict';

const {NettyClientBridge, NettyServerBridge} = require('./netty');
const {SystemError} = require('../types');

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
      const name = preq.clientService.name;
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

module.exports = {
  NettyClientBridge,
  NettyServerBridge,
  Router,
};
