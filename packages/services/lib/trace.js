/* jshint esversion: 6, node: true */

'use strict';

const {SystemError, randomId} = require('./utils');

const debug = require('debug');
const {EventEmitter} = require('events');
const {DateTime, Duration} = require('luxon');

const d = debug('@avro/services:trace');

/**
 * An RPC context thread.
 *
 * Traces are used to control the timing of RPCs, in particular to set
 * deadlines and cancel them early.
 */
class Trace {
  /**
   * Build a new trace.
   *
   * Options:
   *
   * + `free`, the trace will not expire with its parent (but will still share
   *   its headers).
   * + `headers`, initial header values.
   * + `unref`, allow node to terminate before the trace's timeout (if any).
   *   Note that this might cause performance issues if used for too many
   *   traces (see https://nodejs.org/api/timers.html#timers_timeout_unref).
   */
  constructor(timeout, parent, opts) {
    this.deadline = deadlineFromTimeout(timeout);
    if (this.deadline === undefined) {
      opts = parent;
      parent = timeout;
      this.deadline = null;
    }
    if (!Trace.isTrace(parent)) {
      opts = parent;
      parent = undefined;
    }
    opts = opts || {};

    this.headers = parent ? Object.create(parent.headers) : {};
    Object.assign(this.headers, opts.headers);
    this._expiredBy = parent ? parent._expiredBy : undefined;
    this._expiration = null;
    this._children = [];
    this._fns = new Map();
    this._timer = null;

    if (parent) {
      if (!opts.free) {
        parent._children.push(this);
      }
      if (parent.deadline) {
        this.deadline = this.deadline ?
           DateTime.min(this.deadline, parent.deadline) :
           parent.deadline;
      }
    }

    if (!this.expired && this.deadline) {
      const remainingTimeout = +this.deadline.diffNow();
      if (remainingTimeout <= 0) {
        this.expire(deadlineExceededError());
        return;
      }
      if (!parent || this.deadline < parent.deadline) {
        this._timer = setTimeout(() => {
          this.expire(deadlineExceededError());
        }, remainingTimeout);
        if (opts.unref) {
          this._timer.unref();
        }
      }
    }
  }

  get remainingDuration() {
    return this.deadline ? this.deadline.diffNow() : null;
  }

  get expired() {
    return this._expiredBy !== undefined;
  }

  get expiredBy() {
    return this._expiredBy;
  }

  /**
   * Run a callback when the trace expires.
   *
   * Note that the callback might never be run if the trace doesn't have a
   * deadline and is never manually expired. If the trace has already expired,
   * the callback will be run before `whenExpired` returns.
   *
   * `whenExpired` returns a cleanup function which will unregister the
   * callback. It should be called as soon as possible to avoid memory leaks.
   * This cleanup function returns `true` iff the callback has not already been
   * (fully) run.
   */
  whenExpired(fn) {
    if (this.expired) {
      fn.call(this, this._expiredBy);
      return () => false;
    }
    const id = randomId();
    this._fns.set(id, fn);
    return () => this._fns.delete(id);
  }

  /**
   * Manually expire a trace.
   *
   * If the trace has already expired, this method has no effect and will
   * return `false`. Otherwise `expire` will return `true` and the input
   * error--if any--will be forwarded to all expiration callbacks (added via
   * `whenExpired`) and available via `trace.expiredBy`.
   */
  expire(err) {
    if (this.expired) {
      d('Trace has already expired.');
      return false;
    }
    if (this._timer) {
      clearTimeout(this._timer);
      this._timer = null;
    }
    this._expiredBy = err || null;
    for (const [id, fn] of this._fns) {
      fn.call(this, err);
      this._fns.delete(id);
    }
    while (this._children.length) {
      this._children.pop().expire(err);
    }
    return true;
  }

  /**
   * Race a callback against the trace's expiration.
   *
   * Available options:
   *
   * + `expire`, to expire the trace when the returned function is first
   *   called.
   *
   * If the trace has already expired, `fn` will be run on the process' next
   * tick.
   */
  wrap(opts, fn) {
    if (!fn && typeof opts == 'function') {
      fn = opts;
      opts = undefined;
    }
    if (this.expired) {
      process.nextTick(fn, this.expiredBy);
      return;
    }
    const self = this;
    const cleanup = this.whenExpired(done);
    return done;

    function done(err, ...args) {
      if (cleanup()) { // First time we are running this callback.
        if (opts && opts.expire) {
          self.expire(err);
        }
        fn.call(this, err, ...args);
      } else if (err) {
        d('Orphaned error (trace headers: %j).', self.headers, err);
      }
    }
  }

  static isTrace(any) {
    return !!(any && any._isTrace);
  }

  get _isTrace() {
    return true;
  }
}

function deadlineExceededError() {
  return new SystemError('ERR_AVRO_DEADLINE_EXCEEDED');
}

/** Parse various timeout formats into a `DateTime` deadline. */
function deadlineFromTimeout(timeout) {
  if (timeout === undefined || timeout === null) {
    return null;
  }
  if (DateTime.isDateTime(timeout)) {
    return timeout;
  }
  if (typeof timeout == 'number') {
    timeout = Duration.fromMillis(timeout);
  }
  if (Duration.isDuration(timeout)) {
    return DateTime.local().plus(timeout);
  }
  return undefined;
}

module.exports = {
  Trace,
};
