/* jshint esversion: 6, node: true */

'use strict';

const {Channel} = require('./call');
const {Context} = require('./context');
const {Service} = require('./service');
const {SystemError} = require('./types');

const backoff = require('backoff');
const debug = require('debug');
const {EventEmitter} = require('events');

const d = debug('@avro/services:router');

/** A static router. */
class Router {
  constructor(children) {
    if (!children) {
      throw new Error('empty router');
    }
    this._services = [];
    this._children = new Map();
    for (const child of children) {
      for (const svc of child.services) {
        if (this._children.has(svc.name)) {
          throw new Error(`duplicate service name: ${svc.name}`);
        }
        this._children.set(svc.name, child);
        this._services.push(svc);
      }
    }
    this._channel = new Channel((ctx, preq, cb) => {
      const clientSvc = preq.service;
      const names = routingNames(clientSvc);
      let child;
      for (const name of names) {
        const candidate = this._children.get(name);
        if (candidate) {
          if (child) {
            const cause = new Error(`ambiguous service aliases: ${names}`);
            cb(new SystemError('ERR_AVRO_AMBIGUOUS_SERVICE'));
            return;
          }
          child = candidate;
        }
      }
      if (!child) {
        cb(serviceNotFoundError(preq.service));
        return;
      }
      child.channel.call(ctx, preq, cb);
    });
  }

  get channel() {
    return this._channel;
  }

  get services() {
    return this._services.slice();
  }

  static forChannel(chan, svcs) {
    return new ChannelRouter(chan, svcs);
  }

  static forServers(servers) {
    const routers = [];
    for (const server of servers) {
      routers.push(Router.forChannel(server.channel, [server.service]));
    }
    return new Router(routers);
  }

  static pooling(fn, opts, cb) {
    if (!cb && typeof opts == 'function') {
      cb = opts;
      opts = undefined;
    }
    const router = new PoolingRouter(fn, opts)
      .on('error', onError)
      .on('up', onUp);

    function cleanup() {
      router
        .removeListener('error', onError)
        .removeListener('up', onUp);
    }

    function onError(err) {
      cleanup();
      cb(err);
    }

    function onUp() {
      cleanup();
      cb(null, router);
    }
  }
}

class ChannelRouter {
  constructor(chan, svcs) {
    if (!chan || !svcs || !svcs.length) {
      throw new Error('empty channel router');
    }
    this._services = svcs;
    this._serviceNames = new Set();
    for (const svc of svcs) {
      this._serviceNames.add(svc.name); // Speed up later routing check.
    }
    this._channel = new Channel((ctx, preq, cb) => {
      const clientSvc = preq.service;
      if (!isRoutable(clientSvc, this._serviceNames)) {
        cb(serviceNotFoundError(svc));
        return;
      }
      chan.call(ctx, preq, cb);
    });
  }

  get channel() {
    return this._channel;
  }

  get services() {
    return this._services.slice();
  }
}

/** Check whether all services inside a router are reachable. */
function checkHealth(ctx, router, cb) {
  const svcs = router.services;
  const names = svcs.map((svc) => svc.name).join(', ');
  d('Checking the health of %s service(s): %s', svcs.length, names);
  if (ctx.cancelled) {
    cb(ctx.cancelledWith);
    return;
  }
  const cleanup = ctx.onCancel(onPing);
  let pending = svcs.length;
  for (const svc of svcs) {
    router.channel.ping(ctx, svc, onPing);
  }

  function onPing(err, svc) {
    if (err) {
      cb(err);
      pending = 0; // Disable later calls.
      cleanup();
      return;
    }
    d('Service %s is healthy.', svc.name);
    if (--pending > 0) {
      return; // Not done yet.
    }
    cleanup();
    d('All services healthy!')
    cb();
  }
}

class PoolingRouter extends EventEmitter {
  constructor(fn, {context, isFatal, refreshBackoff, size} = {}) {
    if (size && size > 1) {
      throw new Error('not yet supported'); // TODO: Support size > 1.
    }
    super();
    this._routerProvider = fn;
    this._activeRouter = null;
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
        this._setupRouter();
      })
      .on('fail', () => {
        d('Exhausted refresh attempts, giving up.');
        const err = new Error('exhausted refresh attempts');
        this._refreshError = err;
        this.emit('error', err);
      });

    this._setupRouter(context);
  }

  _setupRouter(ctx) {
    this._routerProvider((err, router, ...args) => {
      if (err) {
        d('Error while setting up router: %s', err);
        this._refreshBackoff.backoff();
        return;
      }
      checkHealth(ctx || new Context(), router, (err) => {
        if (err) {
          d('Error while checking router health: %s', err);
          this._refreshBackoff.backoff();
          return;
        }
        this._activeArgs = args;
        this._activeRouter = router; // TODO: Wrap in heartbeat.
        this._attempts = 1;
        this._refreshBackoff.reset();
        d('Pool ready.');
        this.emit('up', ...args);
      });
    });
  }

  _teardownRouter() {
    if (!this._activeArgs) {
      return;
    }
    const args = this._activeArgs;
    this._activeArgs = null;
    this._activeRouter = null;
    this.emit('down', ...args);
  }

  get channel() {
    return new Channel((ctx, preq, cb) => {
      if (!this._activeRouter) {
        const err = this._refreshError;
        if (err) {
          cb(new SystemError('ERR_AVRO_NO_AVAILABLE_CHANNEL', err));
        } else {
          d('Pool empty, queuing call %s.', preq ? preq.id : '*');
          const cleanup = ctx.onCancel(() => {
            this.removeListener('up', onUp);
          });
          const onUp = () => {
            this.removeListener('up', onUp);
            if (cleanup()) {
              this.channel(ctx, preq, cb);
            }
          };
          this.on('up', onUp);
        }
        return;
      }
      this._activeRouter.channel.call(ctx, preq, (err, pres) => {
        const args = this._activeArgs;
        if (err && args && this._isFatal(err) && !this._refreshError) {
          this._teardownRouter();
          this._refreshBackoff.backoff();
        }
        cb(err, pres);
      });
    });
  }

  get services() {
    if (!this._activeRouter) {
      throw new Error('not yet active');
    }
    return this._activeRouter.services;
  }

  destroy() {
    if (!this._refreshError) {
      d('Destroying pool.');
    }
    this._refreshError = new Error('destroyed');
    this._teardownRouter();
  }
}

function serviceNotFoundError(svc) {
  const cause = new Error(`no route for service ${svc.name}`);
  return new SystemError('ERR_AVRO_SERVICE_NOT_FOUND', cause);
}

function routingNames(svc) {
  const keys = [svc.name];
  const aliases = svc.protocol.aliases;
  if (aliases) {
    for (const alias in aliases) {
      keys.push(alias);
    }
  }
  return keys;
}

function isRoutable(clientSvc, svcNames) {
  for (const name of routingNames(clientSvc)) {
    if (svcNames.has(name)) {
      return true;
    }
  }
  return false;
}

module.exports = {
  Router,
};
