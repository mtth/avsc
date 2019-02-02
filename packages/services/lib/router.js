/* jshint esversion: 6, node: true */

'use strict';

const {Server} = require('./call');
const {Channel} = require('./channel');
const {SystemError} = require('./utils');

const backoff = require('backoff');
const debug = require('debug');
const {EventEmitter} = require('events');

const d = debug('@avro/services:router');

/**
 * Base channel routing class.
 *
 * This class should not be instantiated directly. Instead use one of the
 * available factory methods (e.g. `forChannel`, `forServers`).
 */
class Router extends EventEmitter {
  constructor(svcs, handler, routingKeys) {
    if (!svcs || !svcs.length) {
      throw new Error('no services');
    }
    super();
    this.closed = false;
    this._routingKeys = routingKeys || defaultRoutingKeys;
    this._services = new Map();
    for (const svc of svcs) {
      const key = this._routingKeys(svc.protocol)[0];
      if (this._services.has(key)) {
        throw new Error(`duplicate service key: ${key}`);
      }
      this._services.set(key, svc);
    }
    this.channel = new Channel((trace, preq, cb) => {
      if (this.closed) {
        cb(new SystemError('ERR_AVRO_ROUTER_CLOSED'));
        return;
      }
      const clientSvc = preq.service;
      const serverSvc = this.service(clientSvc);
      if (!serverSvc) {
        cb(new SystemError('ERR_AVRO_SERVICE_NOT_FOUND'));
        return;
      }
      handler(serverSvc, trace, preq, cb);
    });
  }

  /** All server services that can be routed to. */
  get services() {
    return Array.from(this._services.values());
  }

  /**
   * Which server service a given client service will be routed to.
   *
   * If no routing key match is found, this method returns `null`.
   */
  service(clientSvc) {
    for (const key of this._routingKeys(clientSvc.protocol)) {
      const svc = this._services.get(key);
      if (svc) {
        return svc;
      }
    }
    return null;
  }

  /** Close the router. */
  close() {
    if (this.closed) {
      return;
    }
    d('Closing router.');
    this.closed = true;
    this.emit('close');
  }

  /**
   * Wrap multiple servers into a single router.
   *
   * Options:
   *
   * + `routingKeys`, a function taking as input a protocol and returning a
   *   (non-empty) array of valid string "routing keys" for the protocol. The
   *   first item will be the protocol's primary key. A client service's
   *   routing keys will be matched in order against all the router's servers'
   *   services' primary keys. This option defaults to a function returning the
   *   protocol's name followed by its aliases if any.
   */
  static forServers(servers, opts) {
    if (!servers || !servers.length) {
      throw new Error('no servers');
    }
    const routers = [];
    for (const server of servers) {
      if (!Server.isServer(server)) {
        throw new Error(`not a server: ${server}`);
      }
      routers.push(Router.forChannel(server.channel, [server.service], opts));
    }
    return routers.length === 1 ?
      routers[0] :
      Router.forRouters(routers, opts);
  }

  /**
   * Combine multiple routers into a single one.
   *
   * All routers must still be open. The list of input routers will be
   * available on the returned router as `router.downstreamRouters`.
   *
   * Options:
   *
   * + `routingKeys`, see `Router.forServer`.
   */
  static forRouters(routers, opts) {
    const routerMap = new Map();
    const svcs = [];
    let upstream;
    for (const downstream of routers) {
      if (downstream.closed) {
        throw new Error('router is already closed');
      }
      downstream.on('close', onClose);
      for (const svc of downstream.services) {
        routerMap.set(svc, downstream);
        svcs.push(svc);
      }
    }
    const handler = (serverSvc, trace, preq, cb) => {
      routerMap.get(serverSvc).channel.call(trace, preq, cb);
    };
    const downstreamRouters = Array.from(routerMap.values());
    upstream = new DispatchingRouter(
      svcs,
      handler,
      opts && opts.routingKeys,
      downstreamRouters
    );
    return upstream;

    function onClose() {
      upstream.close();
      for (const downstream of routerMap.values()) {
        downstream.removeListener('close', onClose);
      }
    }
  }

  /**
   * Wrap a single channel into a router.
   *
   * The input services must be available on the given channel, otherwise calls
   * will likely fail with `ERR_AVRO_INCOMPATIBLE_PROTOCOL`. If you are
   * wrapping a single server's channel, prefer `Router.forServers`.
   *
   * Options:
   *
   * + `routingKeys`, see `Router.forServer`.
   */
  static forChannel(chan, svcs, opts) {
    return new Router(
      svcs,
      (svc, ...args) => { chan.call(...args); },
      opts && opts.routingKeys
    );
  }

  /**
   * Transform a router factory method into a single resilient router.
   *
   * Options:
   *
   * + `refreshBackoff`, `backoff` instance used to throttle provider calls.
   * + `routingKeys`, see `Router.forServer`.
   *
   * The returned router will emit the standard `Router` events as well as:
   *
   * + `'up'`, each time the underlying router is refreshed.
   * + `'down'`, each time the underlying router is closed.
   */
  static selfRefreshing(provider, opts, cb) {
    // TODO: Add `queueBackoff` option.
    if (!cb && typeof opts == 'function') {
      cb = opts;
      opts = undefined;
    }
    const bkf = opts && opts.refreshBackoff || backoff.fibonacci();
    bkf.on('ready', onReady).on('fail', onFail);
    onReady();

    function onReady() {
      provider((err, router, ...args) => {
        if (err) {
          d('Error opening router: %s', err);
          process.nextTick(() => { bkf.backoff(); });
          return;
        }
        bkf
          .removeListener('ready', onReady)
          .removeListener('fail', onFail)
          .reset();
        cb(null, new SelfRefreshingRouter(router, args, provider, bkf));
      });
    }

    function onFail() {
      cb(new Error('unable to open router'));
    }
  }
}

class DispatchingRouter extends Router {
  constructor(svcs, handler, routingKeys, downstreamRouters) {
    super(svcs, handler, routingKeys);
    this.downstreamRouters = downstreamRouters;
  }
}

class SelfRefreshingRouter extends Router {
  constructor(router, args, provider, refreshBackoff) {
    super(router.services, ((svc, trace, preq, cb) => {
      if (this._activeRouter) {
        this._activeRouter.channel.call(trace, preq, cb);
        return;
      }
      const id = preq.id;
      const cleanup = trace.onceInactive(() => {
        this._pendingCalls.delete(id);
      });
      const retry = (err) => {
        if (!cleanup()) {
          d('Ignoring error for packet %s (inactive trace): %s', id, err);
          return;
        }
        this._pendingCalls.delete(id);
        if (err) {
          cb(err);
          return;
        }
        this.channel.call(trace, preq, cb); // Try again.
      };
      this._pendingCalls.set(id, retry);
      this.emit('queue', this._pendingCalls.size);
    }));

    this._routerProvider = provider;
    this._activeRouter = null; // Activated below.
    this._pendingCalls = new Map();
    this._refreshAttempts = 0;
    this._refreshBackoff = refreshBackoff
      .on('backoff', (num, delay) => {
        d('Scheduling refresh in %sms.', delay);
        this._refreshAttempts++;
      })
      .on('ready', () => {
        d('Starting refresh attempt #%s...', this._refreshAttempts);
        this._refreshRouter();
      })
      .on('fail', () => {
        d('Exhausted refresh attempts, giving up.');
        this.emit('error', new Error('exhausted refresh attempts'));
      });

    this.once('close', () => {
      if (this._activeRouter) {
        this._activeRouter.close();
      }
      for (const cb of this._pendingCalls.values()) {
        cb();
      }
    });
    this._activateRouter(router, args);
  }

  _refreshRouter() {
    if (this._activeRouter) {
      throw new Error('router already active');
    }
    d('Refreshing active router...');
    this._routerProvider((err, router, ...args) => {
      if (err) {
        d('Error while opening router: %s', err);
        if (!this.closed) {
          process.nextTick(() => { this._refreshBackoff.backoff(); });
        }
        return;
      }
      if (this.closed) {
        router.close();
        return;
      }
      this._refreshAttempts = 0;
      this._refreshBackoff.reset();
      this._activateRouter(router, args);
    });
  }

  _activateRouter(router, args) {
    this._activeRouter = router
      .on('error', (err) => { this.emit('error', err); })
      .once('close', () => {
        d('Active router was closed.');
        this._activeRouter = null;
        this.emit('down', ...args);
        if (!this.closed) {
          this._refreshRouter();
        }
      });
    d('Set active router.');
    this.emit('up', ...args);
    for (const cb of this._pendingCalls.values()) {
      cb();
    }
  }
}

function defaultRoutingKeys(ptcl) {
  const keys = [ptcl.protocol];
  const aliases = ptcl.aliases;
  if (aliases) {
    for (const alias of aliases) {
      keys.push(alias);
    }
  }
  return keys;
}

module.exports = {
  Router,
};
