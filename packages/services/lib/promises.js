/* jshint esversion: 6, node: true */

'use strict';

const {Client, Server} = require('./call');
const {Trace} = require('./trace');

const Promise = require('bluebird');

class PromisifiedTrace extends Trace {
  constructor(...args) {
    super(...args);
    this._expiration = null;
  }

  get expiration() {
    if (!this._expiration) {
      this._expiration = new Promise((ok, fail) => {
        this.whenExpired((err) => {
          if (err) {
            fail(err);
            return;
          }
          ok(null);
        });
      });
    }
    return this._expiration;
  }
}

/** A client which supports both callback and promise APIs. */
class PromisifiedClient extends Client {
  call(trace, msgName, req, mws, cb) {
    if (!cb && typeof mws == 'function') {
      cb = mws;
      mws = [];
    }
    if (cb) {
      super.call(trace, msgName, req, mws, cb);
      return;
    }
    let reject, resolve, resolveCtx;
    const promise = new Promise(function (resolve_, reject_) {
      resolve = resolve_;
      reject = reject_;
    });
    const ctxPromise = new Promise(function (resolveCtx_) {
      resolveCtx = resolveCtx_; // Never rejected.
    });
    super.call(trace, msgName, req, mws, function (err, res) {
      resolveCtx(this);
      if (err) {
        reject(typeof err.unwrap == 'function' ? err.unwrap() : err);
      } else {
        resolve(res);
      }
    });
    return promise.bind(ctxPromise);
  }

  use(fn) {
    return super.use(promisifyMiddleware(fn));
  }
}

/** A server which supports both callback and promise APIs. */
class PromisifiedServer extends Server {
  onCall(msgName, mws, fn) {
    if (fn.length > 1) { // The handler already has a callback argument.
      return super.onCall(msgName, mws, fn);
    }
    return super.onCall(msgName, mws, function (req, cb) {
      Promise.try(fn.bind(this, req))
        .then((val) => { cb(null, undefined, val); })
        .catch((err) => {
          if (typeof err.wrap != 'function') {
            cb(err);
            return;
          }
          cb(null, err.wrap());
        });
    });
  }

  use(fn) {
    return super.use(promisifyMiddleware(fn));
  }

  client() {
    const client = new PromisifiedClient(this.service);
    client.channel(this.channel());
    return client;
  }
}

function promisifyMiddleware(fn) {
  return function (wreq, wres, next) {
    let reject, resolve, prev;
    const promise = new Promise(function (resolve_, reject_) {
      resolve = resolve_;
      reject = reject_;
    });
    let ret;
    try {
      ret = fn.call(this, wreq, wres, (err, cb) => {
        if (cb) {
          // Always use the callback API if one is provided here.
          next(err, cb);
          return;
        }
        next(err, function (err, prev_) {
          prev = prev_;
          if (err) {
            reject(err);
          } else {
            resolve();
          }
        });
        return promise.bind(this);
      });
    } catch (err) {
      // If an error is thrown synchronously in the handler, we'll be
      // accommodating and assume that this is a promise's rejection.
      next(err);
      return;
    }
    if (fn.length < 3) {
      // The handler didn't have the next argument, so we assume that it was a
      // simple synchronous one.
      next();
      return;
    }
    if (ret && typeof ret.then == 'function') {
      // Cheap way of testing whether `ret` is a promise. If so, we use the
      // promise-based API: we wait until the returned promise is complete to
      // trigger any backtracking.
      ret.then(done, done);
    } else {
      promise.then(done, done);
    }

    function done(err) {
      if (prev) {
        prev(err);
      } else {
        // This will happen if the returned promise is complete before the
        // one returned by `next()` is. There is no clear ideal behavior
        // here, to be safe we will throw an error.
        const cause = new Error('early middleware return');
        promise.finally(function () { prev(cause); });
      }
    }
  };
}

module.exports = {
  PromisifiedClient,
  PromisifiedServer,
  PromisifiedTrace,
};
